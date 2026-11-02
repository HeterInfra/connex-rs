// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the Connex project
//! Small-put rate: batches of 64 async puts of 4 KiB to one peer, all
//! awaited. Benchmark, run with
//! `cargo test --release --test put_rate -- --ignored --nocapture`.
use connex_rs::engine::{Engine, EngineConfig};
use connex_rs::LogicalId;
use std::time::{Duration, Instant};

#[test]
#[ignore]
fn small_put_rate() {
    std::env::set_var(
        "UCX_TLS",
        std::env::var("CONNEX_RS_TEST_TLS").unwrap_or("tcp".into()),
    );
    std::env::set_var("UCX_WARN_UNUSED_ENV_VARS", "n");
    let mk = |s| {
        let mut c = EngineConfig::new(LogicalId::new("pr", s, 0, 0), "127.0.0.1", 0);
        c.spin = Duration::from_micros(200);
        Engine::start(c).unwrap()
    };
    let (a, b) = (mk(0), mk(1));
    let (n, k) = (4096u64, 64u64);
    let src = a.alloc_managed((n * k) as usize).unwrap();
    let dst = b.alloc_managed((n * k) as usize).unwrap();
    let url = b.local_url().to_string();
    let t = Some(Duration::from_secs(5));
    let mut rates = Vec::new();
    for round in 0..12 {
        let t0 = Instant::now();
        let mut ops = 0u64;
        while t0.elapsed() < Duration::from_millis(500) {
            let cs: Vec<_> = (0..k)
                .map(|i| {
                    a.rma_async(&url, true, src + i * n, dst + i * n, n, t, None)
                        .unwrap()
                })
                .collect();
            for c in cs {
                c.wait(None).unwrap().unwrap();
            }
            ops += k;
        }
        if round >= 2 {
            rates.push(ops as f64 / t0.elapsed().as_secs_f64());
        }
    }
    rates.sort_by(|x, y| x.partial_cmp(y).unwrap());
    println!(
        "put_4k_rate_ops_per_s median {:.0} min {:.0} max {:.0}",
        rates[rates.len() / 2],
        rates[0],
        rates[rates.len() - 1]
    );
}
