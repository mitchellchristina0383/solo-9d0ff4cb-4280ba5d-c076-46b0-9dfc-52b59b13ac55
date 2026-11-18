"""灰度发布策略：稳定分组、不可变策略快照、落盘持久化。

设计要点：
  * RolloutPolicy 是不可变快照（frozen dataclass）。网关任何请求在选路时
    只持有当次生效的那一份快照；比例调整 / 关闭灰度只会替换引用，绝不修改
    已在途请求手中的旧快照——因此在途请求始终按原策略、原节点取回响应。
  * 分组函数 bucket() 只依赖 X-Rollout-Key 本身（SHA-256 哈希取模 100），
    不依赖比例、不依赖内存状态。比例不变时重启网关，同一 key 必落同一组。
  * 灰度开启时，bucket < percent 的 key 走 canary（node2），其余走
    baseline（node1）。因此：
      - 上调 percent：只可能把更多组纳入 canary 候选，已在 canary 的 key
        不会被改投；
      - 下调 percent：超出新比例的组被收回到 baseline；
      - percent=0 等价于"开启但零放量"，所有带 key 请求仍走 baseline。
  * 不带 X-Rollout-Key（或值为空 / 全空白）的请求永远走 baseline，
    与灰度开关无关；灰度关闭后全部流量回到原有轮询。
  * 持久化采用"临时文件 + fsync + os.replace"原子替换：写入失败不触碰
    旧文件，下一次加载仍是最后一次成功设置的策略。
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass
from typing import Optional

POLICY_FILE_VERSION = 1
BUCKETS = 100
POLICY_FILENAME = "rollout_policy.json"


# ----------------------------- 策略快照 -----------------------------

@dataclass(frozen=True)
class RolloutPolicy:
    """一份已生效的灰度策略（不可变）。

    enabled=False 时 percent 强制为 0；baseline/canary 为节点名。
    """

    enabled: bool
    percent: int
    baseline_node: str
    canary_node: str
    updated_at: float
    revision: int

    @staticmethod
    def disabled(baseline_node: str, canary_node: str, revision: int = 0) -> "RolloutPolicy":
        return RolloutPolicy(
            enabled=False,
            percent=0,
            baseline_node=baseline_node,
            canary_node=canary_node,
            updated_at=time.time(),
            revision=revision,
        )

    def to_dict(self) -> dict:
        return {
            "version": POLICY_FILE_VERSION,
            "enabled": self.enabled,
            "percent": self.percent,
            "baseline_node": self.baseline_node,
            "canary_node": self.canary_node,
            "updated_at": self.updated_at,
            "revision": self.revision,
        }

    @staticmethod
    def from_dict(data: dict) -> "RolloutPolicy":
        if not isinstance(data, dict):
            raise ValueError("策略文件内容不是 JSON 对象")
        if data.get("version") != POLICY_FILE_VERSION:
            raise ValueError(f"不支持的策略文件版本: {data.get('version')!r}")
        enabled = bool(data["enabled"])
        percent = int(data["percent"])
        if not 0 <= percent <= BUCKETS:
            raise ValueError(f"持久化比例越界: {percent}")
        if not enabled:
            percent = 0
        return RolloutPolicy(
            enabled=enabled,
            percent=percent,
            baseline_node=str(data["baseline_node"]),
            canary_node=str(data["canary_node"]),
            updated_at=float(data["updated_at"]),
            revision=int(data["revision"]),
        )


# ----------------------------- 稳定分组 -----------------------------

def normalize_key(raw: Optional[str]) -> Optional[str]:
    """透传头归一化：None / 空串 / 全空白视为"未标识"。"""
    if raw is None:
        return None
    key = raw.strip()
    return key or None


def bucket(key: str) -> int:
    """把 key 稳定映射到 [0, 99] 的分组号。

    纯函数：只依赖 key 字节，与进程、时间、当前比例均无关，
    因此比例不变时重启网关不会换组。
    """
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest, 16) % BUCKETS


def decide_node(policy: RolloutPolicy, raw_key: Optional[str]) -> tuple[Optional[str], Optional[int]]:
    """按策略决定请求应去的节点名。返回 (节点名, 分组号)。

    灰度关闭、或请求未带标识：节点名为 None（调用方回退轮询），分组号 None。
    灰度开启且带标识：baseline / canary 二选一，分组号一并返回，便于观测。
    """
    key = normalize_key(raw_key)
    if not policy.enabled or key is None:
        return None, None
    b = bucket(key)
    if b < policy.percent:
        return policy.canary_node, b
    return policy.baseline_node, b


# ----------------------------- 持久化 -----------------------------

class PolicyStore:
    """策略落盘：原子写、失败保留旧文件。

    加载阶段文件缺失 -> 返回 None（使用内置默认：灰度关闭）；
    文件损坏 -> 抛异常，由调用方决定是否按默认策略启动并告警。
    """

    def __init__(self, state_dir: str) -> None:
        self.state_dir = state_dir
        self.path = os.path.join(state_dir, POLICY_FILENAME)

    def load(self) -> Optional[RolloutPolicy]:
        if not os.path.exists(self.path):
            return None
        with open(self.path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return RolloutPolicy.from_dict(data)

    def save(self, policy: RolloutPolicy) -> None:
        """原子替换写入。任何失败都不得破坏已有策略文件。"""
        os.makedirs(self.state_dir, exist_ok=True)
        # delete=False：Windows 兼容；Linux 上同样安全，finally 负责清理
        fd, tmp_path = tempfile.mkstemp(
            prefix=".rollout_policy.", suffix=".tmp", dir=self.state_dir
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(policy.to_dict(), fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.path)
            # fsync 目录，保证 rename 元数据落盘
            try:
                dir_fd = os.open(self.state_dir, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                # 个别文件系统不支持目录 fsync，不影响 replace 已完成的事实
                pass
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
