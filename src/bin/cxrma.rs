// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the Connex project
//! cxrma: data-plane microbenchmarks for `connex_rs::engine::Engine`.
//!
//! Measures, per size: blocking get/put latency, bandwidth of one op and of
//! the same bytes split into chunks kept in flight, put_chain latency,
//! time to connect and do a first read, and the CPU the engines burn while
//! idle. Host memory, or GPU memory with `--gpu-owner D` / `--gpu-reader D`.
//!
//!   cxrma local  --ip IP [--sizes 16384,1048576] [--iters N] [--gpu-owner D] [--gpu-reader D]
//!   cxrma owner  --ip IP --port P [--gpu D] [--max B] [--hold S]
//!   cxrma reader --ip IP --url URL --addr A [--gpu D] [...]
//!
//! Output: one JSON object per line on stdout.

use connex_rs::engine::{ChainSpec, Engine, EngineConfig};
use connex_rs::LogicalId;
use std::collections::HashMap;
use std::time::{Duration, Instant};

struct Args(HashMap<String, String>);

impl Args {
    fn parse(v: &[String]) -> Args {
        let mut m = HashMap::new();
        let mut i = 0;
        while i < v.len() {
            let k = v[i].trim_start_matches("--").to_string();
            if i + 1 < v.len() && !v[i + 1].starts_with("--") {
                m.insert(k, v[i + 1].clone());
                i += 2;
            } else {
                m.insert(k, "1".into());
                i += 1;
            }
        }
        Args(m)
    }
    fn get(&self, k: &str, d: &str) -> String {
        self.0.get(k).cloned().unwrap_or_else(|| d.to_string())
    }
    fn num(&self, k: &str, d: u64) -> u64 {
        self.0.get(k).map(|s| s.parse().expect(k)).unwrap_or(d)
    }
    fn opt(&self, k: &str) -> Option<u64> {
        self.0.get(k).map(|s| s.parse().expect(k))
    }
}

// ------------------------------------------------------------------ CUDA

type CuRes = i32;
struct Cuda {
    alloc: unsafe extern "C" fn(*mut u64, usize) -> CuRes,
    htod: unsafe extern "C" fn(u64, *const u8, usize) -> CuRes,
    dtoh: unsafe extern "C" fn(*mut u8, u64, usize) -> CuRes,
}

fn cuda(dev: i32) -> Cuda {
    unsafe {
        let h = libc::dlopen(c"libcuda.so.1".as_ptr(), libc::RTLD_NOW | libc::RTLD_GLOBAL);
        assert!(!h.is_null(), "libcuda.so.1 not found");
        let sym = |n: &std::ffi::CStr| {
            let p = libc::dlsym(h, n.as_ptr());
            assert!(!p.is_null(), "{n:?}");
            p
        };
        let init: unsafe extern "C" fn(u32) -> CuRes = std::mem::transmute(sym(c"cuInit"));
        let get: unsafe extern "C" fn(*mut i32, i32) -> CuRes =
            std::mem::transmute(sym(c"cuDeviceGet"));
        let retain: unsafe extern "C" fn(*mut usize, i32) -> CuRes =
            std::mem::transmute(sym(c"cuDevicePrimaryCtxRetain"));
        let set: unsafe extern "C" fn(usize) -> CuRes =
            std::mem::transmute(sym(c"cuCtxSetCurrent"));
        assert_eq!(init(0), 0);
        let mut d = 0;
        assert_eq!(get(&mut d, dev), 0);
        let mut ctx = 0usize;
        assert_eq!(retain(&mut ctx, d), 0);
        assert_eq!(set(ctx), 0);
        Cuda {
            alloc: std::mem::transmute::<
                *mut libc::c_void,
                unsafe extern "C" fn(*mut u64, usize) -> CuRes,
            >(sym(c"cuMemAlloc_v2")),
            htod: std::mem::transmute::<
                *mut libc::c_void,
                unsafe extern "C" fn(u64, *const u8, usize) -> CuRes,
            >(sym(c"cuMemcpyHtoD_v2")),
            dtoh: std::mem::transmute::<
                *mut libc::c_void,
                unsafe extern "C" fn(*mut u8, u64, usize) -> CuRes,
            >(sym(c"cuMemcpyDtoH_v2")),
        }
    }
}

/// A buffer in host memory (dev < 0) or on GPU `dev`.
struct Buf {
    addr: u64,
    len: usize,
    cu: Option<Cuda>,
}

impl Buf {
    fn new(dev: i64, len: usize) -> Buf {
        if dev < 0 {
            let mut p: *mut libc::c_void = std::ptr::null_mut();
            assert_eq!(unsafe { libc::posix_memalign(&mut p, 4096, len) }, 0);
            Buf {
                addr: p as u64,
                len,
                cu: None,
            }
        } else {
            let cu = cuda(dev as i32);
            let mut a = 0u64;
            assert_eq!(unsafe { (cu.alloc)(&mut a, len) }, 0, "cuMemAlloc");
            Buf {
                addr: a,
                len,
                cu: Some(cu),
            }
        }
    }
    fn write(&self, off: usize, data: &[u8]) {
        match &self.cu {
            None => unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    (self.addr as usize + off) as *mut u8,
                    data.len(),
                )
            },
            Some(c) => assert_eq!(
                unsafe { (c.htod)(self.addr + off as u64, data.as_ptr(), data.len()) },
                0
            ),
        }
    }
    fn read(&self, off: usize, n: usize) -> Vec<u8> {
        let mut v = vec![0u8; n];
        match &self.cu {
            None => unsafe {
                std::ptr::copy_nonoverlapping(
                    (self.addr as usize + off) as *const u8,
                    v.as_mut_ptr(),
                    n,
                )
            },
            Some(c) => assert_eq!(
                unsafe { (c.dtoh)(v.as_mut_ptr(), self.addr + off as u64, n) },
                0
            ),
        }
        v
    }
}

fn pattern(n: usize, seed: u64) -> Vec<u8> {
    (0..n)
        .map(|i| ((i as u64).wrapping_mul(0x9e37_79b9).wrapping_add(seed) >> 3) as u8)
        .collect()
}

fn engine(stage: u32, ip: &str, port: u16) -> Engine {
    let mut cfg = EngineConfig::new(LogicalId::new("cxrma", stage, 0, 0), ip, port);
    if let Some(s) = std::env::var("CXRMA_SPIN_US")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        cfg.spin = Duration::from_micros(s);
    }
    Engine::start(cfg).expect("engine")
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn tls() -> String {
    std::env::var("UCX_TLS").unwrap_or_else(|_| "default".into())
}

/// CPU seconds (user + system) of this process.
fn cpu_s() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let t = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 * 1e-6;
    t(ru.ru_utime) + t(ru.ru_stime)
}

const T: Option<Duration> = Some(Duration::from_secs(10));

/// The reader side: everything is measured here. `remote` is a registered
/// region of at least `max` bytes + 4 KB in the peer at `url`.
/// put_chain ordering check: each chain puts `size` bytes whose u32 words
/// all equal the iteration number, then the flag (a host word in the
/// owner). An observer thread polls the flag like a consumer would and,
/// each time it moves, reads the last data word, which must be at least
/// as new as the flag.
fn verify_chain(
    e: &Engine,
    url: &str,
    remote: u64,
    rbuf: &'static Buf,
    flag: u64,
    local: &Buf,
    size: usize,
    iters: u32,
    odev: i64,
) {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let bad = std::sync::Arc::new(AtomicU64::new(0));
    let seen = std::sync::Arc::new(AtomicU64::new(0));
    unsafe { std::ptr::write_volatile(flag as *mut u32, 0) };
    let (st, b2, s2) = (stop.clone(), bad.clone(), seen.clone());
    let off = (size & !3) - 4;
    let obs = std::thread::spawn(move || {
        if odev >= 0 {
            cuda(odev as i32); // context current for the reads below
        }
        let mut last = 0u32;
        while !st.load(Ordering::Relaxed) {
            let f = unsafe { std::ptr::read_volatile(flag as *const u32) };
            if f != last {
                last = f;
                let w = rbuf.read(off, 4);
                let d = u32::from_le_bytes(w.try_into().unwrap());
                if (d.wrapping_sub(f) as i32) < 0 {
                    b2.fetch_add(1, Ordering::Relaxed);
                }
                s2.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
    for i in 1..=iters {
        let words: Vec<u8> = (0..size / 4).flat_map(|_| i.to_le_bytes()).collect();
        local.write(0, &words);
        let spec = ChainSpec {
            src: local.addr,
            dst: remote,
            len: size as u64,
            flag,
            flag_val: i,
            queue: 2,
            timeout: T,
            ..Default::default()
        };
        e.put_chain(url, spec).unwrap().wait(None).unwrap().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    obs.join().unwrap();
    println!(
        "{{\"type\":\"chain_verify\",\"tls\":\"{}\",\"size\":{size},\"iters\":{iters},\"observed\":{},\"stale\":{}}}",
        tls(),
        seen.load(Ordering::Relaxed),
        bad.load(Ordering::Relaxed)
    );
    assert_eq!(bad.load(Ordering::Relaxed), 0, "flag seen before its data");
}

fn run_reader(a: &Args, e: &Engine, url: &str, remote: u64, dev: i64, mem: &str) {
    let sizes: Vec<usize> = a
        .get("sizes", "4096,16384,65536,1048576,16777216,33554432")
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    let iters = a.num("iters", 1000) as usize;
    let max = *sizes.iter().max().unwrap();
    let local = Buf::new(dev, max + 4096);
    e.register(local.addr, local.len as u64).unwrap();
    let tag = a.get("tag", "");
    let out = |kind: &str, size: usize, body: String| {
        println!(
            "{{\"type\":\"{kind}\",\"tag\":\"{tag}\",\"tls\":\"{}\",\"mem\":\"{mem}\",\"size\":{size},{body}}}",
            tls()
        );
    };

    // first touch: connect + rkey fetch + first read
    let t0 = Instant::now();
    e.connect(url).unwrap();
    let t_conn = t0.elapsed();
    let mut t_up = Duration::ZERO;
    while let Some(ev) = e.wait_event(Some(Duration::from_secs(10))) {
        if matches!(ev, connex_rs::Event::PeerUp { .. }) {
            t_up = t0.elapsed();
            break;
        }
    }
    e.get(url, local.addr, remote, 4096, T).unwrap();
    let t_first = t0.elapsed();
    out(
        "first",
        4096,
        format!(
            "\"connect_ms\":{:.3},\"up_ms\":{:.3},\"first_get_ms\":{:.3}",
            t_conn.as_secs_f64() * 1e3,
            t_up.as_secs_f64() * 1e3,
            t_first.as_secs_f64() * 1e3
        ),
    );

    // correctness of a read and a write at the largest size
    let pat = pattern(max, 7);
    local.write(0, &pat);
    e.put(url, local.addr, remote, max as u64, T).unwrap();
    local.write(0, &vec![0u8; max]);
    e.get(url, local.addr, remote, max as u64, T).unwrap();
    assert!(local.read(0, max) == pat, "put/get roundtrip mismatch");

    for &s in &sizes {
        let n = if s >= 8 << 20 {
            iters.min(50)
        } else if s >= 1 << 20 {
            iters.min(300)
        } else {
            iters
        };
        for (name, is_put) in [("get", false), ("put", true)] {
            for _ in 0..n.min(20) {
                e.rma_async(url, is_put, local.addr, remote, s as u64, T, None)
                    .unwrap()
                    .wait(None)
                    .unwrap()
                    .unwrap();
            }
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                let t = Instant::now();
                e.rma_async(url, is_put, local.addr, remote, s as u64, T, None)
                    .unwrap()
                    .wait(None)
                    .unwrap()
                    .unwrap();
                v.push(t.elapsed().as_secs_f64() * 1e6);
            }
            let p50 = pct(&mut v, 0.5);
            let p99 = pct(&mut v, 0.99);
            out(
                name,
                s,
                format!(
                    "\"p50_us\":{p50:.2},\"p99_us\":{p99:.2},\"GBps\":{:.2}",
                    s as f64 / p50 / 1e3
                ),
            );
        }
        // streaming bandwidth: `depth` ops of `s` bytes kept in flight
        if s >= 1 << 20 {
            let depth = a.num("depth", 4) as usize;
            for (name, is_put) in [("get_bw", false), ("put_bw", true)] {
                let total = n.max(8);
                let t = Instant::now();
                let mut q = std::collections::VecDeque::new();
                for _ in 0..total {
                    if q.len() >= depth {
                        let c: std::sync::Arc<connex_rs::engine::Completion> =
                            q.pop_front().unwrap();
                        c.wait(None).unwrap().unwrap();
                    }
                    q.push_back(
                        e.rma_async(url, is_put, local.addr, remote, s as u64, T, None)
                            .unwrap(),
                    );
                }
                for c in q {
                    c.wait(None).unwrap().unwrap();
                }
                let gbps = (total * s) as f64 / t.elapsed().as_secs_f64() / 1e9;
                out(name, s, format!("\"depth\":{depth},\"GBps\":{gbps:.2}"));
            }
        }
        // put_chain: data put, (readback,) flag put; no doorbell, no gate
        let flag = remote + max as u64;
        let mut v = Vec::with_capacity(n);
        for i in 0..n + n.min(20) {
            let spec = ChainSpec {
                src: local.addr,
                dst: remote,
                len: s as u64,
                flag,
                flag_val: i as u32,
                queue: 1,
                timeout: T,
                ..Default::default()
            };
            let t = Instant::now();
            e.put_chain(url, spec).unwrap().wait(None).unwrap().unwrap();
            if i >= n.min(20) {
                v.push(t.elapsed().as_secs_f64() * 1e6);
            }
        }
        let p50 = pct(&mut v, 0.5);
        let p99 = pct(&mut v, 0.99);
        out(
            "chain",
            s,
            format!("\"p50_us\":{p50:.2},\"p99_us\":{p99:.2}"),
        );
    }

    // idle CPU of this process (the reader's engine and, in local mode, the
    // owner's) while connected and doing nothing
    std::thread::sleep(Duration::from_millis(200));
    let c0 = cpu_s();
    let w = Instant::now();
    std::thread::sleep(Duration::from_secs(a.num("idle-s", 2)));
    let cpu = (cpu_s() - c0) / w.elapsed().as_secs_f64();
    out("idle", 0, format!("\"cpu_cores\":{cpu:.4}"));
    // idle CPU with one put chain armed on a doorbell that never rings
    let words: Box<[u32; 2]> = Box::new([0, 0]); // doorbell, cancel
    let db = words.as_ptr() as u64;
    let armed = e
        .put_chain(
            url,
            ChainSpec {
                doorbell: db,
                doorbell_val: 1,
                cancel: db + 4,
                src: local.addr,
                dst: remote,
                len: 4096,
                queue: 9,
                timeout: T,
                ..Default::default()
            },
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    let c0 = cpu_s();
    let w = Instant::now();
    std::thread::sleep(Duration::from_secs(a.num("idle-s", 2)));
    let cpu = (cpu_s() - c0) / w.elapsed().as_secs_f64();
    unsafe { std::ptr::write_volatile((db + 4) as *mut u32, 1) };
    let _ = armed.wait(None);
    out("idle_armed", 0, format!("\"cpu_cores\":{cpu:.4}"));
    drop(words);
    let st = e.stats();
    out(
        "stats",
        0,
        format!(
            "\"wakeups\":{},\"rma_ok\":{},\"rma_err\":{},\"rkey_fetches\":{}",
            st.wakeups, st.rma_ok, st.rma_err, st.rkey_fetches
        ),
    );
}

fn mem_name(dev: i64) -> String {
    if dev < 0 {
        "host".into()
    } else {
        format!("cuda:{dev}")
    }
}

fn main() {
    let v: Vec<String> = std::env::args().skip(1).collect();
    let Some(mode) = v.first().cloned() else {
        eprintln!("usage: cxrma local|owner|reader ... (see the source header)");
        std::process::exit(2)
    };
    let a = Args::parse(&v[1..]);
    let ip = a.get("ip", "127.0.0.1");
    let max = a.num("max", 32 << 20) as usize + 8192;
    match mode.as_str() {
        "local" => {
            let od = a.opt("gpu-owner").map(|d| d as i64).unwrap_or(-1);
            let rd = a.opt("gpu-reader").map(|d| d as i64).unwrap_or(-1);
            // each engine's progress thread adopts the CUDA context current
            // at start: create the owner's buffer (and context) first
            let ob = Buf::new(od, max);
            let owner = engine(0, &ip, 0);
            owner.register(ob.addr, max as u64).unwrap();
            if rd >= 0 {
                cuda(rd as i32);
            }
            let r = engine(1, &ip, 0);
            if a.0.contains_key("verify-chain") {
                // owner flag in host memory, data in the owner's buffer
                let fb = Buf::new(-1, 4096);
                owner.register(fb.addr, 4096).unwrap();
                let lb = Buf::new(rd, 1 << 20);
                r.register(lb.addr, 1 << 20).unwrap();
                let ob: &'static Buf = Box::leak(Box::new(ob));
                for s in [4096usize, 65536, 1 << 20] {
                    verify_chain(
                        &r,
                        owner.local_url(),
                        ob.addr,
                        ob,
                        fb.addr,
                        &lb,
                        s,
                        a.num("iters", 2000) as u32,
                        od,
                    );
                }
                r.shutdown();
                owner.shutdown();
                return;
            }
            let mem = format!("{}<-{}", mem_name(rd), mem_name(od));
            run_reader(&a, &r, owner.local_url(), ob.addr, rd, &mem);
            r.shutdown();
            owner.shutdown();
        }
        "owner" => {
            let dev = a.opt("gpu").map(|d| d as i64).unwrap_or(-1);
            let b = Buf::new(dev, max);
            let e = engine(0, &ip, a.num("port", 0) as u16);
            e.register(b.addr, max as u64).unwrap();
            println!(
                "{{\"url\":\"{}\",\"addr\":{},\"len\":{max}}}",
                e.local_url(),
                b.addr
            );
            std::thread::sleep(Duration::from_secs(a.num("hold", 600)));
        }
        "reader" => {
            let dev = a.opt("gpu").map(|d| d as i64).unwrap_or(-1);
            if dev >= 0 {
                cuda(dev as i32);
            }
            let e = engine(1, &ip, 0);
            let url = a.get("url", "");
            run_reader(&a, &e, &url, a.num("addr", 0), dev, &mem_name(dev));
            e.shutdown();
        }
        _ => {
            eprintln!("unknown mode {mode}");
            std::process::exit(2)
        }
    }
}
