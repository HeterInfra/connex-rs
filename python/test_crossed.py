# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the Connex project
"""Crossed connects through the Python Engine: a's link-up connect races
b's first read of a (a pipeline's peer watcher and data path). The two
connections must converge on one link: the read succeeds, no failure
event, one "up" per side, and both directions keep working.

usage: PYTHONPATH=<dir with connex_rs> UCX_TLS=tcp python3 python/test_crossed.py [rounds]
"""
import sys
import threading
import time

import connex_rs

N = 64 << 10


def round_(i):
    a = connex_rs.Engine("py", 0, listen_ip="127.0.0.1")
    b = connex_rs.Engine("py", 1, listen_ip="127.0.0.1")
    try:
        sa, da = a.allocate_managed_buffer(N), a.allocate_managed_buffer(N)
        sb, db = b.allocate_managed_buffer(N), b.allocate_managed_buffer(N)
        pa, pb = bytes([i % 251]) * N, bytes([(i + 7) % 251]) * N
        a.write_bytes_to_buffer(sa, pa, N)
        b.write_bytes_to_buffer(sb, pb, N)
        go = threading.Barrier(2)
        rc = {}

        def link():
            go.wait()
            a.connect(b.local_url)

        def read():
            go.wait()
            rc["b"] = b.transfer_sync_read(a.local_url, db, sa, N)

        ts = [threading.Thread(target=link), threading.Thread(target=read)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(30)
        assert rc.get("b", -1) >= 0, f"round {i}: read in flight failed: {rc}"
        assert b.read_bytes_from_buffer(db, N) == pa
        assert a.transfer_sync_read(b.local_url, da, sb, N) >= 0
        assert a.read_bytes_from_buffer(da, N) == pb
        ups = {"a": 0, "b": 0}
        t0 = time.monotonic()
        while time.monotonic() - t0 < 0.3:
            for name, e in (("a", a), ("b", b)):
                ev = e.wait_event(5)
                if ev is None:
                    continue
                assert ev["type"] != "failed", f"round {i} {name}: {ev}"
                ups[name] += ev["type"] == "up"
        assert ups == {"a": 1, "b": 1}, f"round {i}: {ups}"
        return a.stats()["crossed_connects"] + b.stats()["crossed_connects"]
    finally:
        a.close()
        b.close()


if __name__ == "__main__":
    rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    crossed = sum(round_(i) for i in range(rounds))
    print(f"OK: {rounds} rounds, {crossed} crossed")
