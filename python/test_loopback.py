# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the Connex project
"""Loopback with UCX_NET_DEVICES pinned to non-loopback devices used to join,
fail with "Destination is unreachable" and fail again on every reconnect.
connex-rs now refuses such an address up front; with "lo" added it works.

usage: PYTHONPATH=<dir with connex_rs> python3 python/test_loopback.py
"""
import os
import subprocess
import sys


def child():
    import connex_rs
    try:
        connex_rs.Engine("m", 0, listen_ip="127.0.0.1")
    except ValueError as e:
        print("REFUSED", e)
        return
    a = connex_rs.Engine("m", 0, listen_ip="127.0.0.1")
    b = connex_rs.Engine("m", 1, listen_ip="127.0.0.1")
    a.connect(b.local_url)
    a.send_msg(("m", 1, 0, 0), b"hi")
    print("RECV", b.recv_msg(5000))


def run(devices):
    env = dict(os.environ, UCX_NET_DEVICES=devices, UCX_TLS="^cuda_ipc")
    return subprocess.run([sys.executable, __file__, "child"], env=env, capture_output=True,
                          text=True, timeout=60).stdout


if __name__ == "__main__":
    if sys.argv[1:] == ["child"]:
        child()
        sys.exit(0)
    out = run("eth_does_not_matter0")
    assert "REFUSED" in out and '"lo"' in out, out
    out = run("lo")
    assert "RECV (('m', 0, 0, 0), b'hi')" in out, out
    print("OK")
