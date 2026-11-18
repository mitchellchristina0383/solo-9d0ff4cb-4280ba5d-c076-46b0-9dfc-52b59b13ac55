"""HTTP 入口网关（FastAPI + httpx）。

职责：
  * 仅转发 GET 请求到上游；
  * 灰度关闭时按轮询（round-robin）选择「健康且未排空」的节点；
  * 灰度开启时按 X-Rollout-Key 稳定分组：命中比例走 node2（canary），
    其余走 node1（baseline）；不带标识的请求只走 node1；
  * 选路与在途登记在同一把锁内、基于同一份不可变策略快照完成：
    调比例 / 关灰度只替换快照引用，已在途请求始终从原节点取响应；
  * 灰度目标节点故障或排空时返回 503，绝不跨版改投另一节点；
  * 策略设置先原子落盘再切换内存引用：设置失败沿用旧策略，
    重启后自动加载最后一次成功设置的策略；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志；
  * 提供排空 / 恢复 / 状态管理 API；所有节点都不可接流时返回 503；
  * 排空期间新请求不再进入该节点，已在途的请求继续等待原响应返回；
  * 排空 API 阻塞到在途请求全部结束后才报告 drained=true；
  * 超时 / 连接错误 / 5xx 不虚报成功，并同步刷新健康状态，在途计数靠 try/finally 保证准确。
"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from rollout import (
    BUCKETS,
    PolicyStore,
    RolloutPolicy,
    bucket as bucket_of,
    decide_node,
    normalize_key,
)

# ----------------------------- 配置 -----------------------------

UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))
# 灰度基线 / 灰度节点与策略持久化目录
ROLLOUT_BASELINE_NODE = os.environ.get("ROLLOUT_BASELINE_NODE", "node1")
ROLLOUT_CANARY_NODE = os.environ.get("ROLLOUT_CANARY_NODE", "node2")
ROLLOUT_STATE_DIR = os.environ.get("ROLLOUT_STATE_DIR", "/data")

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not _:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), url.strip().rstrip("/")))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


# ----------------------------- 节点状态 -----------------------------

@dataclass
class NodeState:
    name: str
    base_url: str
    # 健康标志：只由探测任务（以及请求失败时的降级）写入
    healthy: bool = False
    # 排空标志：只由人工 drain/resume 写入，探测永不触碰
    draining: bool = False
    inflight: int = 0
    rr_hits: int = 0
    rollout_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    last_change_at: float = field(default_factory=time.time)

    def can_serve(self) -> bool:
        return self.healthy and not self.draining

    def unavailable_reason(self) -> str:
        if self.draining:
            return "node_draining"
        if not self.healthy:
            return "node_unhealthy"
        return "ok"

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "draining": self.draining,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "rr_hits": self.rr_hits,
            "rollout_hits": self.rollout_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
        }


@dataclass(frozen=True)
class PickResult:
    """一次选路结果。node 为 None 时 reason 解释为什么无节点可接流。"""

    node: Optional[NodeState]
    reason: Optional[str]
    # 本次选路依据的策略快照（不可变；在途期间即使策略被替换也不受影响）
    policy: RolloutPolicy
    # route 取值：rr | canary | baseline | baseline-unkeyed
    route: str
    bucket: Optional[int] = None


class RolloutManager:
    """灰度策略的内存持有者 + 持久化。所有读写都在 gateway.cond 锁外/内按约定进行。"""

    def __init__(self, baseline_node: str, canary_node: str, store: PolicyStore) -> None:
        self.baseline_node = baseline_node
        self.canary_node = canary_node
        self.store = store
        self.policy: RolloutPolicy = RolloutPolicy.disabled(baseline_node, canary_node)
        self.last_load_error: Optional[str] = None
        self.last_update_error: Optional[str] = None
        self.last_update_at: Optional[float] = None

    def load_on_startup(self, known_nodes: set[str]) -> None:
        """启动加载最后成功设置的策略；文件缺失或不可用时安全回退到关闭态。"""
        try:
            loaded = self.store.load()
        except FileNotFoundError:
            loaded = None
        except Exception as exc:  # 损坏 / 权限等：不让网关起不来，但如实留痕
            self.last_load_error = f"策略文件加载失败，回退到灰度关闭: {type(exc).__name__}: {exc}"
            print(f"[rollout] {self.last_load_error}")
            loaded = None
        if loaded is None:
            return
        if loaded.baseline_node not in known_nodes or loaded.canary_node not in known_nodes:
            self.last_load_error = (
                f"持久化策略引用了未知节点 "
                f"baseline={loaded.baseline_node!r} canary={loaded.canary_node!r}，回退到灰度关闭"
            )
            print(f"[rollout] {self.last_load_error}")
            return
        if (loaded.baseline_node, loaded.canary_node) != (self.baseline_node, self.canary_node):
            # 节点角色配置与落盘不一致：以当前环境配置为准重建快照，保留开关与比例
            loaded = RolloutPolicy(
                enabled=loaded.enabled,
                percent=loaded.percent,
                baseline_node=self.baseline_node,
                canary_node=self.canary_node,
                updated_at=loaded.updated_at,
                revision=loaded.revision,
            )
        self.policy = loaded
        self.last_update_at = loaded.updated_at
        print(
            f"[rollout] 已加载持久化策略: enabled={loaded.enabled} "
            f"percent={loaded.percent} revision={loaded.revision}"
        )

    def build_next(self, enabled: bool, percent: int) -> RolloutPolicy:
        """基于当前策略构造下一版快照（不落盘）。非法参数抛 ValueError。"""
        percent = int(percent)
        if not 0 <= percent <= BUCKETS:
            raise ValueError(f"percent 必须在 0~{BUCKETS} 之间")
        if not enabled:
            percent = 0
        return RolloutPolicy(
            enabled=enabled,
            percent=percent,
            baseline_node=self.baseline_node,
            canary_node=self.canary_node,
            updated_at=time.time(),
            revision=self.policy.revision + 1,
        )

    def commit(self, new_policy: RolloutPolicy) -> None:
        """落盘成功后在网关锁内调用：切换内存引用。"""
        self.policy = new_policy
        self.last_update_at = new_policy.updated_at
        self.last_update_error = None

    def record_update_error(self, exc: Exception) -> str:
        self.last_update_error = (
            f"策略持久化失败，已沿用旧策略: {type(exc).__name__}: {exc}"
        )
        return self.last_update_error

    def snapshot_dict(self) -> dict:
        p = self.policy
        return {
            "enabled": p.enabled,
            "percent": p.percent,
            "baseline_node": p.baseline_node,
            "canary_node": p.canary_node,
            "revision": p.revision,
            "updated_at": p.updated_at,
            "policy_file": self.store.path,
            "last_load_error": self.last_load_error,
            "last_update_error": self.last_update_error,
        }


class Gateway:
    def __init__(
        self,
        nodes: list[NodeState],
        rollout: RolloutManager,
    ) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        # 一把条件变量保护所有节点状态与策略引用；请求结束 / 排空等待都在其上通知
        self.cond = asyncio.Condition()
        # 串行化策略更新（构造快照 -> 落盘 -> 切换引用），避免并发设置互相覆盖
        self._rollout_update_lock = asyncio.Lock()
        self._rr = 0
        self.rollout = rollout
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    async def pick_node(self, raw_key: Optional[str]) -> PickResult:
        """选路并立即登记在途计数，全程持锁、基于同一份策略快照。

        * 灰度关闭：轮询健康且未排空的节点；
        * 灰度开启 + 带标识：稳定分组指定 baseline/canary，目标不可用即 503，
          绝不跨版改投；
        * 灰度开启 + 无标识：只走 baseline，baseline 不可用即 503。
        """
        async with self.cond:
            policy = self.rollout.policy  # 拿到的是不可变快照引用
            target_name, bkt = decide_node(policy, raw_key)

            if not policy.enabled:
                # 原轮询路径
                n = len(self.nodes)
                for offset in range(n):
                    idx = (self._rr + offset) % n
                    node = self.nodes[idx]
                    if node.can_serve():
                        self._rr = (idx + 1) % n
                        node.inflight += 1
                        node.rr_hits += 1
                        return PickResult(node, None, policy, "rr")
                return PickResult(None, "no_eligible_upstream", policy, "rr")

            # 灰度开启：无标识与 percent=0 的 key 都落在 baseline
            if target_name is None:
                target_name = policy.baseline_node
                route = "baseline-unkeyed"
            else:
                route = "canary" if target_name == policy.canary_node else "baseline"
            node = self._by_name.get(target_name)
            if node is None or not node.can_serve():
                reason = node.unavailable_reason() if node is not None else "unknown_node"
                # 关键：不回退到另一版本节点，直接判定暂不可用
                return PickResult(None, reason, policy, route, bkt)
            node.inflight += 1
            node.rollout_hits += 1
            return PickResult(node, None, policy, route, bkt)

    async def release_node(self, node: NodeState, ok: bool) -> None:
        async with self.cond:
            node.inflight -= 1
            if ok:
                node.success_count += 1
            else:
                node.failure_count += 1
            # 在途归零可能让 drain 等待者满足条件
            self.cond.notify_all()

    async def mark_unhealthy(self, node: NodeState, detail: str) -> None:
        """请求失败时的即时降级；只改健康标志，不动 draining。"""
        async with self.cond:
            node.healthy = False
            node.last_probe_ok = False
            node.last_probe_detail = f"请求失败降级: {detail}"
            node.last_probe_at = time.time()
            node.last_change_at = time.time()

    async def probe_one(self, node: NodeState) -> None:
        """对单个节点做主动探测。成功只置 healthy=True，绝不解除 draining。"""
        assert self.client is not None
        try:
            resp = await self.client.get(
                f"{node.base_url}/", timeout=PROBE_TIMEOUT
            )
            ok = resp.status_code < 500
            detail = f"HTTP {resp.status_code}"
        except httpx.TimeoutException as exc:
            ok, detail = False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            ok, detail = False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

        async with self.cond:
            node.healthy = ok
            # 注意：这里刻意不修改 node.draining
            node.last_probe_at = time.time()
            node.last_probe_ok = ok
            node.last_probe_detail = detail
            node.last_change_at = time.time()

    async def probe_all(self) -> None:
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    async def drain(
        self, node: NodeState, timeout: Optional[float]
    ) -> tuple[bool, int]:
        """置排空标志并等待在途清零。返回 (是否排空完成, 剩余在途数)。"""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            node.draining = True
            node.last_change_at = time.time()
            while node.inflight > 0:
                if deadline is None:
                    await self.cond.wait()
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False, node.inflight
                # asyncio.Condition.wait 不支持 timeout，用 wait_for 限时等待
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # 回到循环重新计算剩余时间 / 在途数
            return True, 0

    async def resume(self, node: NodeState) -> None:
        """恢复接流：先清排空并保守置为不健康，再立即探测，由探测决定能否接流。"""
        async with self.cond:
            node.draining = False
            # 恢复不绕过故障探测：探测结论出来前一律不接流，杜绝状态窗口
            node.healthy = False
            node.last_probe_ok = None
            node.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
        await self.probe_one(node)

    async def apply_rollout(self, enabled: bool, percent: int) -> RolloutPolicy:
        """策略更新完整流程：锁内构造快照 → 线程池原子落盘 → 锁内切换引用。

        落盘失败时旧策略（内存与文件）保持不变，并把失败原因记录在管理器上。
        磁盘 I/O 放在线程池，避免 fsync 阻塞事件循环上的转发请求。
        """
        async with self._rollout_update_lock:
            async with self.cond:
                new_policy = self.rollout.build_next(enabled, percent)
            try:
                await asyncio.to_thread(self.rollout.store.save, new_policy)
            except Exception as exc:
                detail = self.rollout.record_update_error(exc)
                print(f"[rollout] {detail}")
                raise
            async with self.cond:
                self.rollout.commit(new_policy)
            return new_policy


# ----------------------------- 应用生命周期 -----------------------------

def _build_gateway() -> Gateway:
    nodes = [NodeState(name, url) for name, url in _parse_upstreams(UPSTREAMS_RAW)]
    node_names = {n.name for n in nodes}
    missing = [
        n for n in (ROLLOUT_BASELINE_NODE, ROLLOUT_CANARY_NODE) if n not in node_names
    ]
    if missing:
        raise RuntimeError(
            f"灰度节点配置错误: {missing} 不在 UPSTREAMS 节点列表 {sorted(node_names)} 中"
        )
    if ROLLOUT_BASELINE_NODE == ROLLOUT_CANARY_NODE:
        raise RuntimeError("ROLLOUT_BASELINE_NODE 与 ROLLOUT_CANARY_NODE 不能相同")
    store = PolicyStore(ROLLOUT_STATE_DIR)
    rollout = RolloutManager(ROLLOUT_BASELINE_NODE, ROLLOUT_CANARY_NODE, store)
    rollout.load_on_startup(node_names)
    return Gateway(nodes, rollout)


gateway = _build_gateway()


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass


app = FastAPI(
    title="http-entry-gateway",
    version="1.1.0",
    description=(
        "仅转发 GET 的入口网关：支持灰度开关与 0~100% 比例（按 X-Rollout-Key "
        "稳定分组），灰度关闭后轮询健康且未排空的上游，支持排空/恢复/状态管理，"
        "目标节点不可用时返回 503 且不跨版改投。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET，可带 X-Rollout-Key 灰度标识",
        "admin": [
            "GET  /admin/status",
            "GET  /admin/rollout",
            "POST /admin/rollout  body: {\"enabled\": true, \"percent\": 30}",
            "GET  /admin/rollout/classify?key=...",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/probe",
        ],
    }


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(path: str, request: Request) -> Response:
    raw_key = request.headers.get("X-Rollout-Key")
    # 选路与在途登记在同一把锁内基于同一快照完成
    pick = await gateway.pick_node(raw_key)
    if pick.node is None:
        return JSONResponse(
            status_code=503,
            content={
                "error": "rollout_node_unavailable"
                if pick.policy.enabled
                else "no_eligible_upstream",
                "detail": "灰度目标节点暂不可用（故障或排空中），不跨版改投"
                if pick.policy.enabled
                else "当前没有健康且未排空的上游节点可接流",
                "reason": pick.reason,
                "rollout_enabled": pick.policy.enabled,
                "bucket": pick.bucket,
            },
        )

    node = pick.node
    target_url = f"{node.base_url}/{path}"
    ok = False
    try:
        try:
            upstream_resp = await gateway.client.request(
                "GET",
                target_url,
                params=request.query_params,
                timeout=REQUEST_TIMEOUT,
            )
        except httpx.TimeoutException as exc:
            await gateway.mark_unhealthy(node, f"请求超时 {type(exc).__name__}")
            return JSONResponse(
                status_code=504,
                content={"error": "upstream_timeout", "node": node.name},
            )
        except httpx.RequestError as exc:
            await gateway.mark_unhealthy(
                node, f"{type(exc).__name__}: {str(exc)[:100]}"
            )
            return JSONResponse(
                status_code=502,
                content={"error": "upstream_unreachable", "node": node.name},
            )

        # 5xx 视为上游故障：如实透传状态码并降级，绝不虚报成功
        if upstream_resp.status_code >= 500:
            await gateway.mark_unhealthy(
                node, f"上游返回 HTTP {upstream_resp.status_code}"
            )

        ok = upstream_resp.status_code < 500
        headers = {
            k: v
            for k, v in upstream_resp.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        headers["X-Gateway"] = "http-entry-gateway"
        headers["X-Upstream-Node"] = node.name
        # 灰度观测头：本次走的路由与稳定分组号
        headers["X-Rollout-Route"] = pick.route
        if pick.bucket is not None:
            headers["X-Rollout-Bucket"] = str(pick.bucket)
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=headers,
            media_type=upstream_resp.headers.get("content-type"),
        )
    finally:
        # 无论成功、失败还是取消，在途计数都必须准确归还。
        # 注意：归还的是选路时锁定的 node，与策略后续是否变更无关。
        await gateway.release_node(node, ok)


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 灰度管理接口 -----------------------------

class RolloutSetting(BaseModel):
    enabled: bool = Field(..., description="灰度总开关；关闭后流量恢复原轮询")
    percent: int = Field(
        0, ge=0, le=BUCKETS, description="放量比例 0~100：bucket < percent 的 key 走 canary"
    )


@app.get("/admin/rollout", tags=["rollout"])
async def rollout_status() -> dict:
    """查看当前已生效的灰度策略与持久化状态。"""
    async with gateway.cond:
        return gateway.rollout.snapshot_dict()


@app.post("/admin/rollout", tags=["rollout"])
async def set_rollout(setting: RolloutSetting) -> JSONResponse:
    """设置灰度开关与比例。

    先原子落盘再切换内存引用：落盘失败时沿用旧策略（内存与文件均不变），
    返回 500；成功后新请求按新策略选路，已在途请求仍按旧策略从原节点取响应。
    """
    try:
        new_policy = await gateway.apply_rollout(setting.enabled, setting.percent)
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_rollout_setting", "detail": str(exc)},
        )
    except Exception:  # 落盘失败：旧策略保持不变（原因已记录在管理器上）
        return JSONResponse(
            status_code=500,
            content={
                "error": "policy_persist_failed",
                "detail": gateway.rollout.last_update_error,
                "active_policy": gateway.rollout.snapshot_dict(),
            },
        )
    return JSONResponse(
        status_code=200,
        content={
            "applied": True,
            "policy": gateway.rollout.snapshot_dict(),
            "revision": new_policy.revision,
            "detail": "策略已生效并持久化；在途请求仍按旧策略执行完毕",
        },
    )


@app.get("/admin/rollout/classify", tags=["rollout"])
async def classify_key(key: str) -> dict:
    """预览某个 X-Rollout-Key 在当前策略下的分组与目标节点（不发真实流量）。"""
    async with gateway.cond:
        policy = gateway.rollout.policy
        norm = normalize_key(key)
        if norm is None:
            return {
                "key": key,
                "normalized": None,
                "rollout_enabled": policy.enabled,
                "decision": "unkeyed",
                "node": policy.baseline_node if policy.enabled else None,
                "bucket": None,
                "detail": "空标识：灰度开启时只走 baseline，关闭时参与轮询",
            }
        bkt = bucket_of(norm)
        target, _ = decide_node(policy, norm)
        if not policy.enabled:
            decision, node = "round-robin", None
        elif target == policy.canary_node:
            decision, node = "canary", policy.canary_node
        else:
            decision, node = "baseline", policy.baseline_node
        return {
            "key": norm,
            "rollout_enabled": policy.enabled,
            "bucket": bkt,
            "percent": policy.percent,
            "decision": decision,
            "node": node,
        }


# ----------------------------- 节点管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        return {
            "probe_interval": PROBE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
            "nodes": [n.snapshot() for n in gateway.nodes],
            "eligible_nodes": [n.name for n in gateway.nodes if n.can_serve()],
            "rollout": gateway.rollout.snapshot_dict(),
        }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求，并阻塞等待在途请求全部结束。

    只有在途数归零才返回 drained=true；超时仍有在途请求时返回 202，
    drained=false 且节点继续保持排空状态。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    drained, inflight = await gateway.drain(node, timeout)
    body = {
        "node": name,
        "draining": True,
        "drained": drained,
        "inflight": inflight,
        "detail": "在途请求已全部结束" if drained else "等待在途请求结束超时，节点仍处于排空中",
    }
    return JSONResponse(status_code=200 if drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空；是否可接流以健康探测结论为准",
    }


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（探测不会改变任何节点的排空标志）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {"nodes": [n.snapshot() for n in gateway.nodes]}
