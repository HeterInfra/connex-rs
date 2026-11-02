// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the Connex project
//! put_chain gate latency: time from the remote gate opening to the chain's
//! flag landing, with the gate opened 0.5-3 ms after the doorbell (the poll
//! backoff has grown by then). Benchmark, run with
//! `cargo test --release --test gate_latency -- --ignored --nocapture`.
use connex_rs::engine::{ChainSpec, Engine, EngineConfig};
use connex_rs::LogicalId;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

#[test]
#[ignore]
fn gate_open_to_flag_latency() {
    std::env::set_var(
        "UCX_TLS",
        std::env::var("CONNEX_RS_TEST_TLS").unwrap_or("tcp".into()),
    );
    std::env::set_var("UCX_WARN_UNUSED_ENV_VARS", "n");
    let ip = "127.0.0.1";
    let mk = |s| {
        let mut c = EngineConfig::new(LogicalId::new("g", s, 0, 0), ip, 0);
        c.spin = Duration::from_micros(200);
        Engine::start(c).unwrap()
    };
    let (a, b) = (mk(0), mk(1));
    let n = 4096usize;
    let src = a.alloc_managed(n).unwrap();
    let dst = b.alloc_managed(n + 64).unwrap();
    let (flag, gate) = (dst + n as u64, dst + n as u64 + 4);
    let bell = Box::new(AtomicU32::new(0));
    let word = |p: u64| unsafe { &*(p as *const AtomicU32) };
    let mut lat = Vec::new();
    for v in 1..=300u32 {
        let spec = ChainSpec {
            doorbell: bell.as_ref() as *const AtomicU32 as u64,
            doorbell_val: v,
            gate,
            gate_val: v,
            src,
            dst,
            len: n as u64,
            flag,
            flag_val: v,
            queue: 1,
            timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        };
        let c = a.put_chain(b.local_url(), spec).unwrap();
        bell.store(v, Ordering::Release);
        std::thread::sleep(Duration::from_micros(500 + (v as u64 * 7919) % 2500));
        let t0 = Instant::now();
        word(gate).store(v, Ordering::Release);
        while word(flag).load(Ordering::Acquire) != v {
            std::hint::spin_loop();
        }
        if v > 20 {
            lat.push(t0.elapsed().as_secs_f64() * 1e6);
        }
        c.wait(None).unwrap().unwrap();
    }
    lat.sort_by(|x, y| x.partial_cmp(y).unwrap());
    let q = |p: f64| lat[((lat.len() - 1) as f64 * p) as usize];
    println!(
        "gate_open_to_flag_us median {:.1} p90 {:.1} p99 {:.1} max {:.1}",
        q(0.5),
        q(0.9),
        q(0.99),
        lat[lat.len() - 1]
    );
}
