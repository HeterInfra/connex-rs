// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the Connex project
//! Engine tests: real UCX on loopback ($CONNEX_RS_TEST_TLS, default tcp).
use connex_rs::engine::{Engine, EngineConfig, Target};
use connex_rs::{Error, Event, LogicalId};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Sets the UCX environment once: tests run on parallel threads, and a
/// `setenv` while another test's UCX calls `getenv` is a data race.
fn setup() -> String {
    static ENV: std::sync::Once = std::sync::Once::new();
    ENV.call_once(|| {
        let tls = std::env::var("CONNEX_RS_TEST_TLS").unwrap_or_else(|_| "tcp".into());
        std::env::set_var("UCX_TLS", tls);
        std::env::set_var("UCX_WARN_UNUSED_ENV_VARS", "n");
        std::env::set_var("UCX_TCP_CM_REUSEADDR", "y");
    });
    std::env::var("CONNEX_RS_TEST_IP").unwrap_or_else(|_| "127.0.0.1".into())
}

fn engine(ip: &str, stage: u32, port: u16) -> Engine {
    let mut c = EngineConfig::new(LogicalId::new("t", stage, 0, 0), ip, port);
    c.spin = Duration::from_micros(200);
    Engine::start(c).expect("engine")
}

fn fill(e: &Engine, ptr: u64, n: usize, seed: u8) -> Vec<u8> {
    let _ = e;
    let v: Vec<u8> = (0..n)
        .map(|i| (i as u8).wrapping_mul(13).wrapping_add(seed))
        .collect();
    unsafe { std::ptr::copy_nonoverlapping(v.as_ptr(), ptr as *mut u8, n) };
    v
}

fn read(ptr: u64, n: usize) -> Vec<u8> {
    unsafe { std::slice::from_raw_parts(ptr as *const u8, n) }.to_vec()
}

const T: Option<Duration> = Some(Duration::from_secs(5));

#[test]
fn get_put_integrity() {
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = engine(&ip, 1, 0);
    let n = 4 << 20;
    let src = a.alloc_managed(n).unwrap();
    let dst = b.alloc_managed(n).unwrap();
    for (i, &sz) in [64usize, 4096, 16 << 10, 1 << 20, n].iter().enumerate() {
        let want = fill(&a, src, sz, i as u8);
        b.get(a.local_url(), dst, src, sz as u64, T).unwrap();
        assert_eq!(read(dst, sz), want, "get size {sz}");
        // offset inside the region
        b.get(a.local_url(), dst + 8, src + 16, (sz / 2) as u64, T)
            .unwrap();
        assert_eq!(read(dst + 8, sz / 2), want[16..16 + sz / 2].to_vec());
    }
    let want = fill(&b, dst, n, 99);
    b.put(a.local_url(), dst, src, n as u64, T).unwrap();
    assert_eq!(read(src, n), want, "put");
    let s = b.stats();
    assert_eq!(s.rkey_fetches, 1, "one rkey fetch for the whole region");
    a.free_managed(src).unwrap();
    b.free_managed(dst).unwrap();
}

#[test]
fn concurrent_readers() {
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = Arc::new(engine(&ip, 1, 0));
    let n = 64 << 10;
    let src = a.alloc_managed(n * 8).unwrap();
    let want = fill(&a, src, n * 8, 7);
    let url = a.local_url().to_string();
    let hs: Vec<_> = (0..8)
        .map(|t| {
            let b = b.clone();
            let url = url.clone();
            let want = want.clone();
            std::thread::spawn(move || {
                let dst = b.alloc_managed(n).unwrap();
                for _ in 0..200 {
                    b.get(&url, dst, src + (t * n) as u64, n as u64, T).unwrap();
                    assert_eq!(read(dst, n), want[t * n..(t + 1) * n].to_vec());
                }
                b.free_managed(dst).unwrap();
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
}

#[test]
fn unregister_invalidates_cached_rkey() {
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = engine(&ip, 1, 0);
    let n = 1 << 20;
    let dst = b.alloc_managed(n).unwrap();
    // Same address registered, read, unregistered, re-registered: the reader
    // must fetch the new key (a stale key would kill the connection).
    let mut buf = vec![0u8; n];
    let p = buf.as_mut_ptr() as u64;
    for round in 0..20u8 {
        a.register(p, n as u64).unwrap();
        let want = fill(&a, p, n, round);
        b.get(a.local_url(), dst, p, n as u64, T).unwrap();
        assert_eq!(read(dst, n), want);
        a.unregister(p).unwrap();
    }
    assert_eq!(b.stats().rkey_fetches, 20);
    assert_eq!(a.stats().invalidations, 20);
    assert_eq!(a.stats().regions, 0);
    // No failure events along the way.
    while let Some(ev) = b.wait_event(Some(Duration::from_millis(10))) {
        assert!(!matches!(ev, Event::PeerFailed { .. }), "{ev:?}");
    }
}

#[test]
fn unknown_region_fails_cleanly() {
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = engine(&ip, 1, 0);
    let dst = b.alloc_managed(4096).unwrap();
    let r = b.get(a.local_url(), dst, 0x1000, 4096, T);
    assert!(matches!(r, Err(Error::Invalid(_))), "{r:?}");
    // The connection is still good.
    let src = a.alloc_managed(4096).unwrap();
    let want = fill(&a, src, 4096, 3);
    b.get(a.local_url(), dst, src, 4096, T).unwrap();
    assert_eq!(read(dst, 4096), want);
}

#[test]
fn put_signal_orders_data_before_message() {
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = engine(&ip, 1, 0);
    let n = 1 << 20;
    let src = a.alloc_managed(n).unwrap();
    let dst = b.alloc_managed(n).unwrap();
    for i in 0..50u8 {
        let want = fill(&a, src, n, i);
        let c = a
            .put_signal(b.local_url(), src, dst, n as u64, vec![i], T)
            .unwrap();
        c.wait(None).unwrap().unwrap();
        let (from, msg) = b.recv_msg(Some(Duration::from_secs(5))).expect("signal");
        assert_eq!(from, *a.id());
        assert_eq!(msg, vec![i]);
        assert_eq!(read(dst, n), want, "data visible when the signal arrives");
    }
    // plain message back to a peer known by id
    b.send_msg(Target::Id(a.id().clone()), b"pong".to_vec())
        .unwrap();
    assert_eq!(
        a.recv_msg(Some(Duration::from_secs(5))).unwrap().1,
        b"pong".to_vec()
    );
}

#[test]
fn put_chain_waits_for_doorbell_and_gate_then_flags_after_data() {
    use connex_rs::engine::ChainSpec;
    use std::sync::atomic::{AtomicU32, Ordering};
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = engine(&ip, 1, 0);
    let n = 1 << 20;
    let src = a.alloc_managed(n).unwrap();
    let dst = b.alloc_managed(n + 64).unwrap();
    let flag = dst + n as u64; // ready word, then the gate ("free") word
    let gate = flag + 4;
    let bell = Box::new(AtomicU32::new(0));
    let word = |p: u64| unsafe { &*(p as *const AtomicU32) };
    let spec = |v: u32, g: u32| ChainSpec {
        doorbell: bell.as_ref() as *const AtomicU32 as u64,
        doorbell_val: v,
        gate: if g > 0 { gate } else { 0 },
        gate_val: g,
        src,
        dst,
        len: n as u64,
        flag,
        flag_val: v,
        queue: 7,
        timeout: T,
        ..Default::default()
    };
    for v in 1..=20u32 {
        let want = fill(&a, src, n, v as u8);
        let c = a
            .put_chain(b.local_url(), spec(v, v.saturating_sub(1)))
            .unwrap();
        std::thread::sleep(Duration::from_millis(2));
        assert!(!c.is_done(), "ran before the doorbell");
        assert_eq!(word(flag).load(Ordering::Acquire), v - 1);
        if v > 1 {
            // doorbell rung, gate not yet open: still waiting
            bell.store(v, Ordering::Release);
            std::thread::sleep(Duration::from_millis(2));
            assert!(!c.is_done(), "ran before the gate opened");
            word(gate).store(v - 1, Ordering::Release);
        } else {
            bell.store(v, Ordering::Release);
        }
        let t0 = Instant::now();
        while word(flag).load(Ordering::Acquire) != v {
            assert!(t0.elapsed() < Duration::from_secs(5), "flag never came");
        }
        assert_eq!(read(dst, n), want, "data in place when the flag is set");
        c.wait(None).unwrap().unwrap();
    }
    // chains of one queue run in order; the cancel value drops a waiting one
    let c1 = a.put_chain(b.local_url(), spec(30, 0)).unwrap();
    let c2 = a.put_chain(b.local_url(), spec(31, 0)).unwrap();
    bell.store(0x7FFF_FFFF, Ordering::Release);
    assert!(matches!(c1.wait(T), Some(Err(Error::Canceled))));
    assert!(matches!(c2.wait(T), Some(Err(Error::Canceled))));
    assert_eq!(word(flag).load(Ordering::Acquire), 20);
}

#[test]
fn put_chain_cancel_word_wrap_and_deadlines() {
    use connex_rs::engine::ChainSpec;
    use std::sync::atomic::{AtomicU32, Ordering};
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = engine(&ip, 1, 0);
    let n = 8192;
    let src = a.alloc_managed(n).unwrap();
    let dst = b.alloc_managed(n + 64).unwrap();
    let flag = dst + n as u64;
    let gate = flag + 4;
    // Wire up and fetch the rkey first: the chains below have 300 ms
    // deadlines, which RDMA wireup alone can exceed while other tests run.
    a.put(b.local_url(), src, dst, n as u64, T).unwrap();
    let bell = Box::new(AtomicU32::new(0));
    let cancel = Box::new(AtomicU32::new(0));
    let word = |p: u64| unsafe { &*(p as *const AtomicU32) };
    let spec = |v: u32, g: Option<u32>| ChainSpec {
        doorbell: bell.as_ref() as *const AtomicU32 as u64,
        doorbell_val: v,
        gate: if g.is_some() { gate } else { 0 },
        gate_val: g.unwrap_or(0),
        src,
        dst,
        len: n as u64,
        flag,
        flag_val: v,
        queue: 3,
        timeout: Some(Duration::from_millis(300)),
        cancel: cancel.as_ref() as *const AtomicU32 as u64,
        arm_timeout: Some(Duration::from_millis(300)),
    };
    // Counters wrap: u32::MAX - 1, u32::MAX, 0, 1 are reached in order
    // (cyclic comparison, like CU_STREAM_WAIT_VALUE_GEQ).
    bell.store(u32::MAX - 2, Ordering::Release);
    for v in [u32::MAX - 1, u32::MAX, 0, 1, 2] {
        let c = a.put_chain(b.local_url(), spec(v, None)).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        assert!(!c.is_done(), "chain {v} ran before its doorbell");
        bell.store(v, Ordering::Release);
        c.wait(T).unwrap().unwrap();
        assert_eq!(word(flag).load(Ordering::Acquire), v);
    }
    // With a cancel word the doorbell value 0x7FFFFFFF is an ordinary value
    // (a counter passes it), and a nonzero cancel word drops waiting chains
    // even when a late doorbell write landed after the cancel.
    bell.store(0x7FFF_FFFE, Ordering::Release);
    let c = a.put_chain(b.local_url(), spec(0x7FFF_FFFF, None)).unwrap();
    bell.store(0x7FFF_FFFF, Ordering::Release);
    c.wait(T).unwrap().unwrap();
    let c1 = a.put_chain(b.local_url(), spec(0x8000_0001, None)).unwrap();
    let c2 = a.put_chain(b.local_url(), spec(0x8000_0002, None)).unwrap();
    cancel.store(1, Ordering::Release);
    bell.store(0x8000_0002, Ordering::Release); // a late stream write
    assert!(matches!(c1.wait(T), Some(Err(Error::Canceled))));
    assert!(matches!(c2.wait(T), Some(Err(Error::Canceled))));
    assert_eq!(word(flag).load(Ordering::Acquire), 0x7FFF_FFFF);
    cancel.store(0, Ordering::Release);
    // A doorbell that never comes times out (arm_timeout) instead of
    // holding its queue forever.
    let t0 = Instant::now();
    let c = a.put_chain(b.local_url(), spec(0x9000_0000, None)).unwrap();
    assert!(matches!(c.wait(T), Some(Err(Error::Timeout))));
    assert!(t0.elapsed() < Duration::from_secs(2));
    // A gate that never opens times out too (polled with backoff).
    word(gate).store(0, Ordering::Release);
    bell.store(0x9000_0001, Ordering::Release);
    let t0 = Instant::now();
    let c = a
        .put_chain(b.local_url(), spec(0x9000_0001, Some(5)))
        .unwrap();
    assert!(matches!(c.wait(T), Some(Err(Error::Timeout))));
    let dt = t0.elapsed();
    assert!(
        dt >= Duration::from_millis(250) && dt < Duration::from_secs(2),
        "{dt:?}"
    );
    // ... and the queue goes on afterwards
    word(gate).store(6, Ordering::Release);
    let c = a
        .put_chain(b.local_url(), spec(0x9000_0001, Some(5)))
        .unwrap();
    c.wait(T).unwrap().unwrap();
    assert_eq!(word(flag).load(Ordering::Acquire), 0x9000_0001);
}

#[test]
fn peer_failure_and_same_identity_rejoin() {
    let ip = setup();
    let b = engine(&ip, 1, 0);
    let a = engine(&ip, 0, 0);
    let port = a.port();
    let url = a.local_url().to_string();
    let n = 1 << 16;
    let src = a.alloc_managed(n).unwrap();
    let dst = b.alloc_managed(n).unwrap();
    fill(&a, src, n, 1);
    b.get(&url, dst, src, n as u64, T).unwrap();
    // "kill" a: shutdown closes its endpoints; b must see a failure event.
    let t0 = Instant::now();
    a.shutdown();
    let mut failed = false;
    while t0.elapsed() < Duration::from_secs(5) {
        if let Some(Event::PeerFailed { peer: Some(p), .. }) =
            b.wait_event(Some(Duration::from_millis(100)))
        {
            assert_eq!(p, LogicalId::new("t", 0, 0, 0));
            failed = true;
            break;
        }
    }
    assert!(failed, "no PeerFailed after peer shutdown");
    let detect = t0.elapsed();
    assert!(detect < Duration::from_secs(2), "slow detection {detect:?}");
    // Reads of the dead peer fail fast.
    let r = b.get(&url, dst, src, n as u64, Some(Duration::from_secs(2)));
    assert!(r.is_err());
    // Same identity, same port: b's reads work again without restarting b.
    let a2 = engine(&ip, 0, port);
    assert_eq!(a2.local_url(), url);
    let src2 = a2.alloc_managed(n).unwrap();
    let want = fill(&a2, src2, n, 42);
    let mut ok = false;
    for _ in 0..50 {
        if b.get(&url, dst, src2, n as u64, T).is_ok() {
            ok = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(ok, "read after rejoin");
    assert_eq!(read(dst, n), want);
}

#[test]
fn leak_check_many_reconnects() {
    let ip = setup();
    let b = engine(&ip, 1, 0);
    let dst = b.alloc_managed(4096).unwrap();
    let mut port = 0;
    for i in 0..30 {
        let a = engine(&ip, 0, port);
        port = a.port();
        let src = a.alloc_managed(4096).unwrap();
        let want = fill(&a, src, 4096, i as u8);
        let mut ok = false;
        for _ in 0..100 {
            if b.get(a.local_url(), dst, src, 4096, T).is_ok() {
                ok = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ok, "cycle {i}");
        assert_eq!(read(dst, 4096), want);
        drop(a);
    }
    std::thread::sleep(Duration::from_millis(300));
    let s = b.stats();
    assert!(s.ep.live_peers <= 1, "{s:?}");
    assert!(s.remote_rkeys <= 1, "{s:?}");
    assert_eq!(s.inflight, 0);
    assert!(s.ep.eps_created - s.ep.eps_closed <= 1, "{s:?}");
}

#[test]
fn links_both_ways_are_one_endpoint_and_report_up() {
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = engine(&ip, 1, 0);
    // b reads from a (b -> a), then a links to b: reuses the inbound link.
    let src = a.alloc_managed(4096).unwrap();
    let dst = b.alloc_managed(4096).unwrap();
    b.get(a.local_url(), dst, src, 4096, T).unwrap();
    a.connect(b.local_url()).unwrap();
    b.connect(a.local_url()).unwrap();
    b.get(a.local_url(), dst, src, 4096, T).unwrap();
    let mut up_a = false;
    let mut up_b = false;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_millis(500) {
        if let Some(ev) = a.wait_event(Some(Duration::from_millis(20))) {
            assert!(!matches!(ev, Event::PeerFailed { .. }), "a: {ev:?}");
            up_a |= matches!(ev, Event::PeerUp { .. });
        }
        if let Some(ev) = b.wait_event(Some(Duration::from_millis(20))) {
            assert!(!matches!(ev, Event::PeerFailed { .. }), "b: {ev:?}");
            up_b |= matches!(ev, Event::PeerUp { .. });
        }
    }
    assert!(up_a && up_b);
    assert_eq!(a.stats().ep.eps_created, 1);
    assert_eq!(b.stats().ep.eps_created, 1);
    // A connect to a port nobody listens on fails without a PeerUp.
    let c = engine(&ip, 2, 0);
    let dead = connex_rs::engine::PeerUrl::format(&ip, 1, &LogicalId::new("t", 9, 0, 0));
    let _ = c.connect(&dead);
    let mut saw_up = false;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_millis(500) {
        if let Some(Event::PeerUp { .. }) = c.wait_event(Some(Duration::from_millis(50))) {
            saw_up = true;
        }
    }
    assert!(!saw_up);
}

/// Both sides connect at the same moment (a's link-up connect crossing b's
/// first read of a, as a pipeline's peer watcher and data path do). The two
/// connections must converge on one link: no failure event, PeerUp exactly
/// once per side, the read in flight completes, and both directions work.
#[test]
fn crossed_connects_converge() {
    let ip = setup();
    let mut crossed = 0;
    for round in 0..20u32 {
        let a = engine(&ip, 0, 0);
        let b = engine(&ip, 1, 0);
        let n = 64 << 10;
        let (src_a, dst_a) = (a.alloc_managed(n).unwrap(), a.alloc_managed(n).unwrap());
        let (src_b, dst_b) = (b.alloc_managed(n).unwrap(), b.alloc_managed(n).unwrap());
        let want_a = fill(&a, src_a, n, round as u8);
        let want_b = fill(&b, src_b, n, round as u8 ^ 0x5a);
        let go = std::sync::Barrier::new(2);
        std::thread::scope(|s| {
            s.spawn(|| {
                go.wait();
                a.connect(b.local_url()).unwrap();
            });
            s.spawn(|| {
                go.wait();
                if round % 2 == 0 {
                    b.get(a.local_url(), dst_b, src_a, n as u64, T).unwrap();
                } else {
                    b.connect(a.local_url()).unwrap();
                    b.get(a.local_url(), dst_b, src_a, n as u64, T).unwrap();
                }
            });
        });
        assert_eq!(read(dst_b, n), want_a, "round {round}: read in flight");
        a.get(b.local_url(), dst_a, src_b, n as u64, T).unwrap();
        assert_eq!(read(dst_a, n), want_b, "round {round}: a reads b");
        b.get(a.local_url(), dst_b, src_a, n as u64, T).unwrap();
        let (mut up_a, mut up_b) = (0, 0);
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_millis(300) {
            while let Some(ev) = a.wait_event(Some(Duration::from_millis(5))) {
                assert!(
                    !matches!(ev, Event::PeerFailed { .. }),
                    "round {round} a: {ev:?}"
                );
                up_a += matches!(ev, Event::PeerUp { .. }) as u32;
            }
            while let Some(ev) = b.wait_event(Some(Duration::from_millis(5))) {
                assert!(
                    !matches!(ev, Event::PeerFailed { .. }),
                    "round {round} b: {ev:?}"
                );
                up_b += matches!(ev, Event::PeerUp { .. }) as u32;
            }
        }
        assert_eq!((up_a, up_b), (1, 1), "round {round}: PeerUp once per side");
        assert_eq!(a.stats().ep.live_peers, 1);
        assert_eq!(b.stats().ep.live_peers, 1);
        crossed += a.stats().ep.crossed_connects + b.stats().ep.crossed_connects;
    }
    assert!(crossed > 0, "no round actually crossed");
}

// ---------------------------------------------------------------- review regressions

/// Runs `f` on a thread and fails (instead of hanging) if it does not
/// return within `t`.
fn within<R: Send + 'static>(t: Duration, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(t).expect("call did not return in time")
}

#[test]
fn recv_msg_returns_after_shutdown() {
    let ip = setup();
    let a = Arc::new(engine(&ip, 0, 0));
    let a2 = a.clone();
    let h = std::thread::spawn(move || a2.recv_msg(None));
    std::thread::sleep(Duration::from_millis(100));
    a.shutdown();
    let r = within(Duration::from_secs(5), move || h.join().unwrap());
    assert!(r.is_none());
}

#[test]
fn huge_timeouts_do_not_panic() {
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = engine(&ip, 1, 0);
    let src = a.alloc_managed(4096).unwrap();
    let dst = b.alloc_managed(4096).unwrap();
    let want = fill(&a, src, 4096, 5);
    let max = Some(Duration::MAX);
    b.get(a.local_url(), dst, src, 4096, max).unwrap();
    assert_eq!(read(dst, 4096), want);
    let c = b
        .rma_async(a.local_url(), false, dst, src, 4096, max, None)
        .unwrap();
    assert_eq!(c.wait(max), Some(Ok(())));
    assert!(b.recv_msg(Some(Duration::from_millis(1))).is_none());
    assert!(a.wait_event(Some(Duration::from_secs(u64::MAX))).is_some());
}

#[test]
fn rma_length_overflow_is_rejected() {
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = engine(&ip, 1, 0);
    let src = a.alloc_managed(8192).unwrap();
    let dst = b.alloc_managed(8192).unwrap();
    // Prime the rkey cache so the lookup runs on the cached region.
    b.get(a.local_url(), dst, src, 8192, T).unwrap();
    // remote + len wraps around to inside the region
    let r = b.get(a.local_url(), dst, src + 16, u64::MAX - 8, T);
    assert!(matches!(r, Err(Error::Invalid(_))), "{r:?}");
    let r = b.get(a.local_url(), dst, src, 0, T);
    assert!(r.is_ok() || matches!(r, Err(Error::Invalid(_))), "{r:?}");
    // still usable
    let want = fill(&a, src, 8192, 9);
    b.get(a.local_url(), dst, src, 8192, T).unwrap();
    assert_eq!(read(dst, 8192), want);
}

#[test]
fn unregister_waits_for_local_inflight_rma() {
    let ip = setup();
    let a = engine(&ip, 0, 0);
    let b = engine(&ip, 1, 0);
    let n: usize = 256 << 20;
    let src = a.alloc_managed(n).unwrap();
    let want = fill(&a, src, n, 1);
    let mut buf = vec![0u8; n];
    let dst = buf.as_mut_ptr() as u64;
    b.register(dst, n as u64).unwrap();
    // Warm up (connection + rkey).
    b.get(a.local_url(), dst, src, 4096, T).unwrap();
    let c = b
        .rma_async(
            a.local_url(),
            false,
            dst,
            src,
            n as u64,
            Some(Duration::from_secs(30)),
            None,
        )
        .unwrap();
    // The local buffer's registration must outlive the read using it.
    b.unregister(dst).unwrap();
    assert_eq!(c.wait(None), Some(Ok(())));
    assert!(read(dst, n) == want, "data intact");
    drop(buf);
}
