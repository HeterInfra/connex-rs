# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the Connex project
"""Batch RMA API: rma_batch / transfer_batch_sync move the same bytes as
one call per op, and report failures; with --bench, ops/s of 64 x 4 KiB puts
per batch against put_async + wait per op.

usage: PYTHONPATH=<dir with connex_rs> UCX_TLS=tcp python3 python/test_batch.py [--bench]
"""
import os
import sys
import time

import connex_rs

N, K = 4096, 64


def main():
    a = connex_rs.Engine("bt", 0, listen_ip="127.0.0.1")
    b = connex_rs.Engine("bt", 1, listen_ip="127.0.0.1")
    src = a.allocate_managed_buffer(N * K)
    dst = b.allocate_managed_buffer(N * K)
    payload = os.urandom(N * K)
    a.write_bytes_to_buffer(src, payload, len(payload))
    ops = [(src + i * N, dst + i * N, N) for i in range(K)]
    assert a.transfer_batch_sync(b.local_url, True, ops, timeout_ms=5000) == 0
    assert bytes(b.read_bytes_from_buffer(dst, N * K)) == payload
    back = a.allocate_managed_buffer(N * K)
    ts = a.rma_batch(b.local_url, False, [(back + i * N, dst + i * N, N) for i in range(K)],
                     timeout_ms=5000)
    for t in ts:
        assert t.wait(5000)
    assert bytes(a.read_bytes_from_buffer(back, N * K)) == payload
    # a read outside any registered region fails, the others do not hang
    rc = a.transfer_batch_sync(b.local_url, False, [(back, dst, N), (back, 8, N)],
                               timeout_ms=2000)
    assert rc < 0, rc
    print("batch API OK")
    if "--bench" in sys.argv:
        for name in ("per_op", "batch"):
            rates = []
            for _ in range(5):
                t0, n = time.perf_counter(), 0
                while time.perf_counter() - t0 < 0.5:
                    if name == "batch":
                        assert a.transfer_batch_sync(b.local_url, True, ops, timeout_ms=5000) == 0
                    else:
                        hs = [a.put_async(b.local_url, s, d, ln, timeout_ms=5000)
                              for s, d, ln in ops]
                        for h in hs:
                            h.wait(5000)
                    n += K
                rates.append(n / (time.perf_counter() - t0))
            rates.sort()
            print(f"{name} put_4k_ops_per_s median {rates[2]:.0f}")


if __name__ == "__main__":
    main()
