use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use transfer::{Completion, Config, Engine, Permissions, RemoteRegion};

fn engine() -> Engine {
    Engine::new(Config {
        limit: 4,
        timeout: Duration::from_secs(5),
        frame: 4096,
        token: b"process-test".to_vec(),
    })
    .unwrap()
}

struct Peer(Child);

impl Drop for Peer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn worker() {
    if std::env::var_os("TRANSFERPEER").is_none() {
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let engine = engine();
        let region = engine.register(vec![7; 128], Permissions::READWRITE);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        println!(
            "descriptor {}",
            region.remote(listener.local_addr().unwrap())
        );
        std::io::stdout().flush().unwrap();
        let stop = CancellationToken::new();
        let serving = engine.clone();
        let token = stop.clone();
        let task = tokio::spawn(async move { serving.serve(listener, token).await });
        tokio::task::spawn_blocking(|| {
            let mut line = String::new();
            std::io::stdin().read_line(&mut line).unwrap();
            assert_eq!(line.trim(), "stop");
        })
        .await
        .unwrap();
        assert_eq!(region.read(0, 128).unwrap(), vec![9; 128]);
        stop.cancel();
        task.await.unwrap().unwrap();
    });
}

#[test]
fn processes() {
    let mut previous: Option<RemoteRegion> = None;
    for _ in 0..2 {
        let mut peer = Peer(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "worker", "--nocapture"])
                .env("TRANSFERPEER", "1")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let output = peer.0.stdout.take().unwrap();
        let mut output = BufReader::new(output);
        let remote: RemoteRegion = (&mut output)
            .lines()
            .map(Result::unwrap)
            .find_map(|line| {
                line.split_once("descriptor ")
                    .map(|(_, value)| value.to_owned())
            })
            .expect("worker descriptor")
            .parse()
            .unwrap();
        let stale = previous.replace(remote);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let engine = engine();
            let target = engine.register(vec![0; 128], Permissions::READWRITE);
            if let Some(mut stale) = stale {
                assert_ne!(stale.id, remote.id);
                stale.endpoint = remote.endpoint;
                let read = engine.read(stale, 0, &target, 0, 128).await.unwrap();
                assert!(matches!(read.wait().await, Completion::Failed(_)));
                assert_eq!(target.read(0, 128).unwrap(), vec![0; 128]);
            }
            let read = engine.read(remote, 0, &target, 0, 128).await.unwrap();
            assert!(matches!(
                read.wait().await,
                Completion::Success { bytes: 128 }
            ));
            assert_eq!(target.read(0, 128).unwrap(), vec![7; 128]);
            target.write(0, &[9; 128]).unwrap();
            let write = engine.write(&target, 0, remote, 0, 128).await.unwrap();
            assert!(matches!(
                write.wait().await,
                Completion::Success { bytes: 128 }
            ));
        });
        writeln!(peer.0.stdin.as_mut().unwrap(), "stop").unwrap();
        assert!(peer.0.wait().unwrap().success());
    }
}
