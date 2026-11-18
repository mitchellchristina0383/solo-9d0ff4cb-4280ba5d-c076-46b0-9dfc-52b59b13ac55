#!/usr/bin/env python3
"""灰度发布端到端自测（不依赖 docker，stdlib + 三个 uvicorn 子进程）。

覆盖场景：
  R1. 默认灰度关闭：行为与原轮询一致
  R2. 100% 放量：带 X-Rollout-Key 的请求全部走 node2，无标识只走 node1
  R3. 稳定分组：同 key 恒定同节点，响应头带分组号
  R4. 比例单调：上调只增加候选组；下调按新比例收回；回调后集合复原
  R5. 0% 放量：开启状态下所有 key 仍走 node1
  R6. canary 排空：灰度请求 503，不跨版改投 node1；在途慢请求仍取回 node2 响应
  R7. canary 故障（error 模式）：探测摘除后灰度请求 503，不虚报、不改投
  R8. 在途请求与改策略交错：调比例/关灰度不影响在途，新请求按新策略
  R9. 策略持久化与重启：重启后沿用最后成功策略，且同 key 不换组
  R10. 持久化失败：返回 500 且内存沿用旧策略
  R11. 关闭灰度后恢复原轮询
  R12. classify 预览接口与非法比例校验
  R13. baseline 排空时无标识请求 503（不切 node2）
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

PYLIBS = os.environ.get("PYLIBS", "/tmp/pylibs")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable

GW_PORT = 19180
U1_PORT = 19181
U2_PORT = 19182

passed = 0
failed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def http(method: str, url: str, timeout: float = 20.0,
         data: bytes | None = None, headers: dict | None = None):
    req = urllib.request.Request(url, method=method, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()


def hget(hdrs: dict, name: str) -> str | None:
    """大小写不敏感取响应头（h11 回传的头名为小写）。"""
    if not hdrs:
        return None
    low = name.lower()
    for k, v in hdrs.items():
        if k.lower() == low:
            return v
    return None


def jget(url: str, headers: dict | None = None, timeout: float = 20.0):
    status, hdrs, body = http("GET", url, timeout, headers=headers)
    return status, hdrs, json.loads(body)


def post_json(url: str, payload: dict, timeout: float = 10.0):
    return http("POST", url, data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"}, timeout=timeout)


def wait_ready(url: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = http("GET", url, timeout=2)
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def spawn(module_dir: str, port: int, extra_env: dict | None = None):
    launcher = (
        "import sys,runpy,os;"
        f"sys.path[:0]=[{PYLIBS!r}, os.getcwd()];"
        "sys.argv=['uvicorn']+sys.argv[1:];"
        "runpy.run_module('uvicorn', run_name='__main__')"
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.update(extra_env or {})
    proc = subprocess.Popen(
        [PYTHON, "-I", "-c", launcher, "app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=module_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    return proc


def kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=8)
        except Exception:
            proc.kill()


def node_status(node: str) -> dict:
    status, _, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
    assert status == 200
    return next(n for n in body["nodes"] if n["name"] == node)


def rollout_status() -> dict:
    status, _, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/rollout")
    assert status == 200, body
    return body


def set_rollout(enabled: bool, percent: int):
    return post_json(f"http://127.0.0.1:{GW_PORT}/admin/rollout",
                     {"enabled": enabled, "percent": percent})


def proxy_echo(key: str | None = None):
    headers = {"X-Rollout-Key": key} if key is not None else None
    return jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo", headers=headers)


def set_mode(port: int, mode: str) -> None:
    status, _, body = http("POST", f"http://127.0.0.1:{port}/control/mode?mode={mode}")
    assert status == 200, body


def wait_eligible(node: str, want: bool, timeout: float = 8.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if node_status(node)["eligible"] is want:
            return True
        time.sleep(0.2)
    return False


def main() -> int:
    procs = []
    state_dir = tempfile.mkdtemp(prefix="rollout-state-")
    try:
        print("== 启动两个上游和网关（独立策略目录 %s）==" % state_dir)
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        u2 = spawn(os.path.join(ROOT, "upstream"), U2_PORT, {"NODE_ID": "node2"})
        procs = [u1, u2]
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U2_PORT}/")

        gw_env = {
            "UPSTREAMS": f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT}",
            "PROBE_INTERVAL": "1",
            "PROBE_TIMEOUT": "1",
            "REQUEST_TIMEOUT": "15",
            "DRAIN_DEFAULT_TIMEOUT": "10",
            "ROLLOUT_STATE_DIR": state_dir,
        }

        def spawn_gw():
            p = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
            assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs"), "网关启动失败"
            time.sleep(1.2)
            return p

        gw = spawn_gw()
        procs.append(gw)

        # R1. 默认灰度关闭，轮询不变
        print("== R1. 默认灰度关闭 ==")
        st = rollout_status()
        check("初始 enabled=False percent=0", st["enabled"] is False and st["percent"] == 0, str(st))
        seen = [proxy_echo()[2]["node"] for _ in range(4)]
        check("轮询交替不变", seen == ["node1", "node2", "node1", "node2"], str(seen))

        # R2. 100% 放量
        print("== R2. 100% 放量 ==")
        status, _, body_raw = set_rollout(True, 100)
        body = json.loads(body_raw)
        check("设置 100% 成功", status == 200 and body["applied"] is True, body)
        st = rollout_status()
        check("策略显示 enabled=True percent=100",
              st["enabled"] is True and st["percent"] == 100, str(st))
        keyed = [proxy_echo(f"term-{i}")[2]["node"] for i in range(10)]
        check("带标识请求全部走 node2", all(n == "node2" for n in keyed), str(keyed))
        unkeyed = [proxy_echo()[2]["node"] for _ in range(5)]
        check("无标识请求只走 node1", all(n == "node1" for n in unkeyed), str(unkeyed))
        blank = [proxy_echo("   ")[2]["node"] for _ in range(3)]
        check("空白标识视同无标识走 node1", all(n == "node1" for n in blank), str(blank))

        # R3. 稳定分组 + 观测头
        print("== R3. 稳定分组 ==")
        key = "stable-terminal-xyz"
        nodes = set()
        buckets = set()
        for _ in range(6):
            status, hdrs, body = proxy_echo(key)
            assert status == 200
            nodes.add(body["node"])
            buckets.add(hget(hdrs, "X-Rollout-Bucket"))
        check("同一 key 恒定同一节点", nodes == {"node2"}, str(nodes))
        check("分组号恒定且在 0~99", len(buckets) == 1 and buckets.pop().isdigit(), str(buckets))
        status, hdrs, _ = proxy_echo()
        check("无标识请求无 bucket 头、route=baseline-unkeyed",
              hget(hdrs, "X-Rollout-Bucket") is None
              and hget(hdrs, "X-Rollout-Route") == "baseline-unkeyed",
              str(hdrs))

        # R4. 比例单调性（用 classify 预览，不发真实流量）
        print("== R4. 比例上调/下调的集合语义 ==")
        keys = [f"device-{i:04d}" for i in range(300)]

        def canary_set(percent: int) -> set[str]:
            status, _, _ = set_rollout(True, percent)
            assert status == 200
            out = set()
            for k in keys:
                s, _, b = jget(f"http://127.0.0.1:{GW_PORT}/admin/rollout/classify?key={k}")
                assert s == 200
                if b["decision"] == "canary":
                    out.add(k)
            return out

        s30 = canary_set(30)
        s60 = canary_set(60)
        s30_again = canary_set(30)
        check("30% 实际占比接近设定值", 0.20 <= len(s30) / len(keys) <= 0.40,
              f"{len(s30)}/{len(keys)}")
        check("上调到 60% 只增候选组", s30.issubset(s60) and len(s60) > len(s30),
              f"{len(s30)} -> {len(s60)}")
        check("调回 30% 集合完全复原（按新比例收回）", s30 == s30_again)
        check("被收回的 key 改走 baseline",
              all(
                  jget(f"http://127.0.0.1:{GW_PORT}/admin/rollout/classify?key={k}")[2]["node"] == "node1"
                  for k in (s60 - s30)
              ))

        # R5. 0% 放量
        print("== R5. 0% 放量 ==")
        status, _, _ = set_rollout(True, 0)
        assert status == 200
        nodes = {proxy_echo(f"zero-{i}")[2]["node"] for i in range(8)}
        check("0% 时所有带标识请求走 node1", nodes == {"node1"}, str(nodes))
        status, _, _ = set_rollout(True, 100)
        assert status == 200

        # R6. canary 排空：503 不改投 + 在途请求完整
        print("== R6. node2 排空中的灰度行为 ==")
        result = {}

        def slow_to_node2():
            result["status"], result["headers"], result["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3",
                headers={"X-Rollout-Key": "drain-inflight-key"}, timeout=20,
            )

        t = threading.Thread(target=slow_to_node2)
        t.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node2")["inflight"] != 1:
            time.sleep(0.1)
        check("慢请求已在 node2 在途", node_status("node2")["inflight"] == 1)
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=10")
        b = json.loads(body)
        check("排空等待在途结束后成功", status == 200 and b["drained"] is True, body)
        t.join(timeout=20)
        check("在途请求仍从 node2 取回完整 200",
              result.get("status") == 200 and json.loads(result["body"])["node"] == "node2",
              str(result))
        status, _, body = proxy_echo("should-503-key")
        check("排空后灰度请求返回 503", status == 503, body)
        check("503 标明原因且未跨版改投",
              body["reason"] == "node_draining"
              and body["rollout_enabled"] is True, body)
        # 恢复 node2
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/resume")
        assert status == 200
        check("恢复后灰度请求重新走 node2", wait_eligible("node2", True)
              and proxy_echo("back-to-canary")[2]["node"] == "node2")

        # R7. canary 故障：503 不改投
        print("== R7. node2 故障时的灰度行为 ==")
        set_mode(U2_PORT, "error")
        check("探测摘除 node2", wait_eligible("node2", False))
        status, _, body = proxy_echo("failover-forbidden-key")
        check("canary 故障时灰度请求 503", status == 503, body)
        check("原因 node_unhealthy，绝不改投 node1",
              body["reason"] == "node_unhealthy", body)
        set_mode(U2_PORT, "healthy")
        check("node2 恢复后重新可接流", wait_eligible("node2", True))

        # R8. 在途请求与改策略交错
        print("== R8. 在途期间调整比例 / 关闭灰度 ==")
        status, _, _ = set_rollout(True, 100)
        assert status == 200
        inflight = {}

        def slow_call():
            inflight["status"], inflight["headers"], inflight["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3",
                headers={"X-Rollout-Key": "policy-switch-key"}, timeout=20,
            )

        t = threading.Thread(target=slow_call)
        t.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node2")["inflight"] != 1:
            time.sleep(0.1)
        check("在途请求登记在 node2", node_status("node2")["inflight"] == 1)
        # 在途期间先下调到 0，再直接关闭灰度
        status, _, _ = set_rollout(True, 0)
        check("在途期间下调比例成功", status == 200)
        status, _, body = proxy_echo("policy-switch-key")
        check("新请求按新比例走 node1", status == 200 and body["node"] == "node1", body)
        status, _, _ = set_rollout(False, 0)
        check("关闭灰度成功", status == 200)
        rr_seen = [proxy_echo()[2]["node"] for _ in range(4)]
        check("关闭后新请求恢复两节点轮询",
              sorted(set(rr_seen)) == ["node1", "node2"], str(rr_seen))
        t.join(timeout=20)
        check("在途请求不受策略变更影响，仍从 node2 取回",
              inflight.get("status") == 200
              and json.loads(inflight["body"])["node"] == "node2"
              and hget(inflight["headers"], "X-Upstream-Node") == "node2",
              str(inflight))
        check("结束后在途归零",
              all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")))

        # R9. 持久化 + 重启
        print("== R9. 重启沿用最后成功策略且不换组 ==")
        status, _, _ = set_rollout(True, 45)
        assert status == 200
        before = {}
        for k in ("persist-a", "persist-b", "persist-c", "persist-d"):
            s, _, b = jget(f"http://127.0.0.1:{GW_PORT}/admin/rollout/classify?key={k}")
            before[k] = (b["bucket"], b["decision"], b["node"])
        rev_before = rollout_status()["revision"]
        kill(gw)
        procs.remove(gw)
        gw = spawn_gw()
        procs.append(gw)
        st = rollout_status()
        check("重启后策略仍为 enabled/45%",
              st["enabled"] is True and st["percent"] == 45, str(st))
        check("revision 保持连续", st["revision"] == rev_before, str(st))
        after = {}
        for k in before:
            s, _, b = jget(f"http://127.0.0.1:{GW_PORT}/admin/rollout/classify?key={k}")
            after[k] = (b["bucket"], b["decision"], b["node"])
        check("比例不变时重启不换组", before == after, f"{before} vs {after}")
        # 真实流量同样稳定
        live = {k: proxy_echo(k)[2]["node"] for k in before}
        check("真实流量节点与预览一致",
              all(live[k] == before[k][2] for k in before), str(live))
        check("策略文件确实落盘", os.path.exists(os.path.join(state_dir, "rollout_policy.json")))

        # R10. 持久化失败沿用旧策略
        print("== R10. 持久化失败 ==")
        os.chmod(state_dir, 0o500)  # 目录只读 -> mkstemp 失败
        try:
            status, _, body = post_json(
                f"http://127.0.0.1:{GW_PORT}/admin/rollout",
                {"enabled": True, "percent": 90},
            )
            check("落盘失败返回 500", status == 500, body)
            check("响应中带回仍生效的旧策略",
                  json.loads(body)["active_policy"]["percent"] == 45, body)
        finally:
            os.chmod(state_dir, 0o700)
        st = rollout_status()
        check("内存沿用旧策略 45%", st["enabled"] is True and st["percent"] == 45, str(st))
        check("失败留痕可查询", st["last_update_error"] is not None, str(st))
        check("旧策略下流量行为不变", proxy_echo("persist-a")[2]["node"] == before["persist-a"][2])

        # R11. 关闭灰度恢复轮询（重启后再验证一次）
        print("== R11. 关闭灰度并重启 ==")
        status, _, _ = set_rollout(False, 0)
        assert status == 200
        kill(gw)
        procs.remove(gw)
        gw = spawn_gw()
        procs.append(gw)
        st = rollout_status()
        check("重启后灰度保持关闭", st["enabled"] is False and st["percent"] == 0, str(st))
        seen = [proxy_echo(f"rr-{i}")[2]["node"] for i in range(4)]
        check("带标识请求也回到轮询", sorted(set(seen)) == ["node1", "node2"], str(seen))

        # R12. classify 与参数校验
        print("== R12. classify 预览与非法参数 ==")
        status, _, body = jget("http://127.0.0.1:{}/admin/rollout/classify?key=abc".format(GW_PORT))
        check("关闭态 classify 显示 round-robin",
              status == 200 and body["decision"] == "round-robin", body)
        status, _, body = post_json(
            f"http://127.0.0.1:{GW_PORT}/admin/rollout",
            {"enabled": True, "percent": 101},
        )
        check("percent=101 被拒（422）", status == 422, body)
        status, _, body = post_json(
            f"http://127.0.0.1:{GW_PORT}/admin/rollout",
            {"enabled": True, "percent": -1},
        )
        check("percent=-1 被拒（422）", status == 422, body)
        status, _, body = post_json(
            f"http://127.0.0.1:{GW_PORT}/admin/rollout",
            {"percent": 10},
        )
        check("缺 enabled 被拒（422）", status == 422, body)

        # R13. baseline 排空：无标识请求 503，不切 node2（50% 放量使两组都存在）
        print("== R13. baseline 排空时无标识请求 ==")
        status, _, _ = set_rollout(True, 50)
        assert status == 200
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        assert status == 200
        status, _, body = proxy_echo()
        check("无标识请求在 baseline 排空时 503", status == 503, body)
        check("不跨版改投 node2", body["reason"] == "node_draining", body)
        # 低比例 key（bucket>=percent）同样依赖 baseline，也应 503
        low_key = next(
            f"low-{i}" for i in range(500)
            if jget(f"http://127.0.0.1:{GW_PORT}/admin/rollout/classify?key=low-{i}")[2]["decision"] == "baseline"
        )
        status, _, body = proxy_echo(low_key)
        check("baseline 组 key 在 node1 排空时 503", status == 503, body)
        # canary 组不受影响
        canary_key = next(
            f"can-{i}" for i in range(500)
            if jget(f"http://127.0.0.1:{GW_PORT}/admin/rollout/classify?key=can-{i}")[2]["decision"] == "canary"
        )
        status, _, body = proxy_echo(canary_key)
        check("canary 组 key 仍由 node2 正常服务", status == 200 and body["node"] == "node2", body)
        http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        wait_eligible("node1", True)

        print(f"\n==== 结果: {passed} passed, {failed} failed ====")
        return 1 if failed else 0

    finally:
        for p in procs:
            kill(p)
        try:
            os.chmod(state_dir, 0o700)
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
