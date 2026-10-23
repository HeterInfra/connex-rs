# Contributing to connex-rs

connex-rs is the data plane of [Connex](https://github.com/HeterInfra/connex).
Bug reports, fixes and measurements on new hardware are welcome; please open
an issue before large changes so the design can be discussed first.

## Build and test

You need a Rust toolchain (rustup, stable) and UCX headers and libraries
(`libucx-dev` on Debian/Ubuntu, or HPC-X). `UCX_HOME` points at the UCX
prefix (default `/opt/hpcx/ucx`, else `/usr`).

```bash
cargo fmt --check
cargo clippy --release --all-targets --all-features -- -D warnings
cargo test --release                      # UCX over TCP on loopback
maturin build --release -o dist           # Python wheel (abi3)
python scripts/check_release.py           # release scanner, also run in CI
```

CI runs the same steps on GitHub runners (no RDMA or GPU). Changes to the
RDMA or GPU paths should say in the pull request on which hardware they were
tested (`CONNEX_RS_TEST_TLS`/`CONNEX_RS_TEST_IP` run the tests over RDMA, see
the README).

## Pull requests

- Keep each pull request to one change, with tests where it is testable.
- Add a line to `CHANGELOG.md` for user-visible changes.
- Do not commit hostnames, addresses, paths or tokens from your environment;
  the release scanner rejects them.
- By contributing you agree that your contribution is licensed under the
  Apache License, Version 2.0 (see `LICENSE`).

Please follow the [Code of Conduct](CODE_OF_CONDUCT.md), and report security
problems as described in [SECURITY.md](SECURITY.md).
