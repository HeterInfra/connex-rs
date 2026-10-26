# Changelog

## 0.1.2

- Fix a sender crash during wire-up across hosts: the 1-byte heartbeat
  payload lived in read-only memory, and when UCX sent it zero-copy (low
  zero-copy threshold without a registration cache) registering it with the
  NIC failed. The buffer is now writable.

## 0.1.1

- Python: `Engine.put_async`, a posted one-sided write that returns a handle
  to wait on later (used by the Connex SGLang PD transfer backend for
  non-blocking KV writes).

## 0.1.0

First release of connex-rs as its own repository (previously
`native/connex-rs` in the Connex repository), the default data plane of
[Connex](https://github.com/HeterInfra/connex) on RDMA hosts.

- Rust crate over UCX (`csrc/cxshim.c` compiled against the system UCX;
  `$UCX_HOME`, else `/opt/hpcx/ucx`, else `/usr`) and a PyO3 binding
  `connex_rs` (abi3 wheel, Python >= 3.8, built with maturin).
- `connex_rs.Engine`: one-sided RMA (get/put) on registered host or GPU
  memory with GPUDirect RDMA, `put_signal` (write, then a message that
  arrives after the data), plain messages, on a dedicated progress thread;
  the GIL is released while waiting. Implements the TransferEngine surface
  that `connex.pp` uses.
- Identity-keyed peers (`ip:port#model|stage|shard|replica`): a stage
  restarted under the same identity rejoins without restarting its chain.
- Peer-failure events within milliseconds from UCX endpoint errors;
  heartbeat (default 1 s / 3 s) and per-op timeouts for hung peers.
- Integration tests over real UCX (threads and processes), failure-detection
  benchmark (`cxbench`), RMA and churn scripts in `python/`.
- PyPI and crates.io package name: `connex-rs`.
