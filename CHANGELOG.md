# Changelog

## Unreleased (0.1.5)

- RMA from or into registered GPU memory below UCX's zero-copy threshold is
  rejected with an error instead of crashing. Below that threshold UCX takes
  the short / bcopy protocols, which copy the buffer with the CPU: a 2 KB
  put from GPU memory segfaulted in `uct_rc_mlx5_ep_put_short` /
  `put_bcopy` (HPC-X 2.18, rc_mlx5), also inside `put_chain`. The threshold
  is `UCX_ZCOPY_THRESH`; connex-rs now sets it to 4096 on its UCX config
  when the variable is unset, so the limit is known (`gpu_rma_min()`). The
  0.1.4 "known limit" note is replaced by this check.
- `put_chain`: a `cancel` word (a local u32 only the CPU writes; nonzero
  cancels the chain while it waits for its doorbell or gate). The 0.1.4
  cancel value 0x7FFFFFFF in the doorbell could be overwritten by a stream
  write that was still queued; without a cancel word the old behaviour
  stays. Doorbell and gate comparisons are now cyclic (`(int32)(word - val)
  >= 0`, as `CU_STREAM_WAIT_VALUE_GEQ`), so counters may wrap past 2^32 and
  pass 0x7FFFFFFF.
- `put_chain` deadlines: `arm_timeout` bounds the doorbell wait (default
  none), and the whole gate wait is bounded by `timeout` (it polled
  forever). A gate that is not open is read again after a growing pause
  (2 µs doubling to 200 µs) instead of back to back, and the progress
  thread's poll interval while chains are armed grows with the idle time
  (5 µs, then 10 µs after 1 ms, 20 µs after 5 ms) instead of 5 µs forever.
- A cancelled request that UCX has not completed after the cancel is kept,
  with its buffer, until UCX completes it (it was freed after a bounded
  spin even if UCX could still write the buffer). `Stats.cancel_deferred`
  counts them.
- Python: `Engine.put_chain(..., cancel=0, arm_timeout_ms=None)`.
- An RMA or `put_chain` whose address range wraps around the address
  space (`addr + len` overflows) is rejected with an invalid-argument error.
  It used to pass the region lookup (the end wrapped to inside the region)
  and was posted with a huge length; over TCP the serving peer segfaulted.
- The CUDA driver is looked up again (at most every 100 ms) until found,
  under a lock. It was looked up once per process: an engine created before
  CUDA was initialised (Python: `connex_rs.Engine` before `torch.cuda`)
  left that and every later engine without CUDA support, so GPU memory was
  registered as host memory (no GPUDirect, `cuMemGetAddressRange` errors,
  and the small-GPU-RMA check above did not apply).
- `cx_forget_ep` walked the event ring with a 64-bit index against the
  32-bit head/tail counters; after 2^32 events it never terminated.
- `Engine.recv_msg(None)` returns None once the engine is closed (it waited
  forever). `wait_event` / `recv_msg` waiters are woken under their locks at
  shutdown, so none can miss it.
- At shutdown endpoints are force-closed before callers are told their
  operations were canceled, so no RDMA can land in buffers they then free.
- The command eventfd is closed when the last reference to the engine's
  shared state goes away, not in `shutdown`: a command racing `shutdown`
  could write to a reused descriptor.
- An unregister waits for this engine's own in-flight RMA from or into the
  region before unmapping it.
- Timeouts too large for `Instant` (e.g. `Duration::MAX`) mean "no
  deadline" instead of panicking.
- `Endpoint::listen` a second time fails (it replaced and leaked the first
  listener, which kept accepting); a listener whose address query fails is
  destroyed. `Endpoint::connect` validates the address before it drops the
  current link to the peer. The shim checks its event ring allocation.
- `CONNEX_RS_DEBUG` is read once, not on every event.
- Python: `stats()` reports `cancel_deferred` (both classes) and
  `closes_pending` / `anon_eps` (Engine); heartbeat defaults no longer
  overflow for huge intervals; docstrings list the `up` event and the -5
  (canceled) return code.
- Tests: the put_chain deadline test wires up before its 300 ms chains (it
  failed on rc_mlx5 when run with the other tests); the UCX environment is
  set once (concurrent `setenv` / `getenv` is a data race); CI runs clippy
  without features too; `rust-version = "1.82"`.
- README: heartbeat is off unless configured, crossed connects converge on
  the lower id's connection, return code -5, the GPU RMA size limit.

## 0.1.4

- `Engine.put_chain` (Rust and Python): an armed put that the progress
  thread runs on its own. It waits until a u32 in local memory (a
  "doorbell", typically written by a CUDA stream with
  `cuStreamWriteValue32`) reaches a value, optionally until a u32 in the
  peer's memory reaches a value (polled with one-sided reads; a credit
  gate for ring slots), then puts the data, reads its last word back, and
  puts a u32 flag to the peer. The peer can wait for the flag on the GPU
  (`cuStreamWaitValue32` on registered host memory) and use the data with
  no application thread on either side involved. The read-back makes the
  data visible before the flag also on GPUs that do not order inbound RDMA
  writes (GDR flush, as NCCL does). Chains with the same `queue` run in
  order; the value 0x7FFFFFFF in the doorbell or the gate cancels. While
  chains are armed the progress thread polls every few microseconds
  instead of sleeping (its timer slack is now 1 µs). Used by Connex's
  pipeline-parallel plugin for cross-host stage hops.
- Known limit: a very small put from GPU memory (16 bytes here) takes
  UCX's inline path, which reads the source with the CPU and crashes (HPC-X
  2.18 UCX, rc_mlx5, also with the memory handle given); stage small
  payloads in host memory. 16 KB and larger from GPU memory is fine.

## 0.1.3

- Simultaneous connects between two peers (each side connecting to the
  other at once, e.g. a link-up connect crossing the other side's first
  read) now converge on one link. Both sides keep the connection opened by
  the lower logical id: the higher side switches to it (no rejoin, its own
  connection closes after its in-flight operations complete, cached rkeys
  are fetched again), the lower side parks the other connection until the
  peer closes it. Previously each side replaced the other's endpoint: an
  in-flight read failed, a live peer was reported reset, and the engine
  could miss `PeerUp`. The engine now reports `PeerUp` exactly once per
  link. No wire change, and the decision needs no message exchange. With a
  0.1.2 peer, a crossing converges when the 0.1.2 side has the higher id;
  when it has the lower id it replaces the link as before (same failure as
  between two 0.1.2 peers, no worse).
- `Stats.crossed_connects` counts tie-breaks.
- Fix heap corruption when an `Op` was dropped while still posted (neither
  waited for nor canceled): its buffer was freed, and a later message with
  the posted receive's tag (e.g. from the peer's next incarnation) was
  written into freed memory. Dropping a pending op now leaks its buffer
  instead; `Endpoint::wait` with a timeout remains the way to give one up.
- Endpoints are identified by address, and UCX reuses a freed endpoint's
  address for the next one (typically the accepted reconnect of a
  restarted peer). Closing an endpoint now drops its still-queued events,
  so they cannot be taken for the new one, and a bind never closes an "old"
  endpoint that is the new one at the same address. Previously a restarted
  peer could have each new connection closed right after its hello.
- Tests: the heartbeat test starts its healthy window once the link is up
  (wire-up can exceed the window on loaded CI runners); the process-level
  SIGKILL test binds its listeners to port 0 instead of a fixed port that
  could already be in use.

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
