use std::error::Error;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use transfer::{Completion, Config, Direction, Engine, Permissions, RemoteRegion, Transfer};

const TOKEN: &[u8] = b"transfer-bench";
const LIMIT: usize = 16;
const CONCURRENCY: usize = 8;
const REMOTE: usize = 64 << 20;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server = Engine::new(Config {
        limit: LIMIT,
        timeout: Duration::from_secs(30),
        frame: 16 << 20,
        token: TOKEN.to_vec(),
    })?;
    let client = Engine::new(Config {
        limit: LIMIT,
        timeout: Duration::from_secs(30),
        frame: 16 << 20,
        token: TOKEN.to_vec(),
    })?;

    let remote = server.register(vec![7; REMOTE], Permissions::READWRITE);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let stop = CancellationToken::new();
    let serving = server.clone();
    let token = stop.clone();
    tokio::spawn(async move { serving.serve(listener, token).await });
    let region = remote.remote(addr);

    println!(
        "tcp host={} cpu={} limit={} concurrency={} remote={} MiB",
        std::env::consts::OS,
        std::env::consts::ARCH,
        LIMIT,
        CONCURRENCY,
        REMOTE >> 20
    );

    for (size, ops) in [(4 << 10, 20_000), (64 << 10, 2_000), (1 << 20, 200)] {
        warmup(&client, region, size).await?;
        let read = measure(&client, region, size, ops, Direction::Read).await?;
        let write = measure(&client, region, size, ops, Direction::Write).await?;
        print(size, ops, "read", &read);
        print(size, ops, "write", &write);
    }

    stop.cancel();
    Ok(())
}

struct Stats {
    wall: Duration,
    cpu: Duration,
    latency: Vec<Duration>,
    peak: u64,
}

async fn warmup(client: &Engine, remote: RemoteRegion, size: usize) -> Result<(), Box<dyn Error>> {
    let target = client.register(vec![0; size], Permissions::READWRITE);
    for _ in 0..16 {
        let handle = client.read(remote, 0, &target, 0, size).await?;
        success(handle.wait().await);
    }
    Ok(())
}

async fn measure(
    client: &Engine,
    remote: RemoteRegion,
    size: usize,
    ops: usize,
    direction: Direction,
) -> Result<Stats, Box<dyn Error>> {
    let before = usage();
    let start = Instant::now();
    let mut tasks = JoinSet::new();
    for worker in 0..CONCURRENCY {
        let client = client.clone();
        tasks.spawn(async move {
            let data = if direction == Direction::Read {
                vec![0; size]
            } else {
                vec![(worker + 1) as u8; size]
            };
            let permissions = if direction == Direction::Read {
                Permissions::WRITE
            } else {
                Permissions::READ
            };
            let region = client.register(data, permissions);
            let offset = (worker * size) as u64;
            let mut latency = Vec::with_capacity(ops);
            for _ in 0..ops {
                let transfer = Transfer {
                    direction,
                    region: region.clone(),
                    offset: 0,
                    remote,
                    remote_offset: offset,
                    len: size,
                };
                let started = Instant::now();
                let handle = client.submit(transfer).await.unwrap();
                success(handle.wait().await);
                latency.push(started.elapsed());
            }
            latency
        });
    }

    let mut latency = Vec::with_capacity(ops * CONCURRENCY);
    while let Some(result) = tasks.join_next().await {
        latency.extend(result?);
    }
    let wall = start.elapsed();
    let after = usage();
    latency.sort_unstable();
    Ok(Stats {
        wall,
        cpu: after.0.saturating_sub(before.0),
        latency,
        peak: after.1.max(before.1),
    })
}

fn success(completion: Completion) {
    assert!(
        matches!(completion, Completion::Success { .. }),
        "{completion:?}"
    );
}

fn print(size: usize, ops: usize, direction: &str, stats: &Stats) {
    let total = ops * CONCURRENCY;
    let secs = stats.wall.as_secs_f64();
    let gib = (total * size) as f64 / (1 << 30) as f64;
    let p50 = percentile(&stats.latency, 50);
    let p99 = percentile(&stats.latency, 99);
    println!(
        "size={:>4} KiB dir={direction} ops={total:>6} throughput={:>8.2} GiB/s ops/s={:>9.0} p50={:>9.1} us p99={:>9.1} us cpu={:>7.2} s peak={:>7} MiB",
        size >> 10,
        gib / secs,
        total as f64 / secs,
        p50,
        p99,
        stats.cpu.as_secs_f64(),
        stats.peak >> 20,
    );
}

fn percentile(samples: &[Duration], percent: usize) -> f64 {
    let index = (samples.len() * percent / 100).min(samples.len().saturating_sub(1));
    samples[index].as_secs_f64() * 1e6
}

#[cfg(unix)]
fn usage() -> (Duration, u64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided struct on success.
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
    }
    // SAFETY: the struct was initialized by the successful call above.
    let usage = unsafe { usage.assume_init() };
    let time =
        |value: libc::timeval| Duration::new(value.tv_sec as u64, value.tv_usec as u32 * 1_000);
    let cpu = time(usage.ru_utime) + time(usage.ru_stime);
    let rss = usage.ru_maxrss as u64;
    let peak = if cfg!(target_os = "macos") {
        rss
    } else {
        rss * 1024
    };
    (cpu, peak)
}

#[cfg(not(unix))]
fn usage() -> (Duration, u64) {
    (Duration::ZERO, 0)
}
