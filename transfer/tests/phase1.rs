use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use transfer::{Completion, Config, Engine, Error, Permissions, RemoteRegion, Transfer};

const TOKEN: &[u8] = b"phase1-secret";
const BLOCK: usize = 64;

struct Fixture {
    client: Engine,
    server: Engine,
    addr: SocketAddr,
    stop: CancellationToken,
}

fn make(token: &[u8], timeout: Duration) -> Engine {
    Engine::new(Config {
        limit: 16,
        timeout,
        frame: 1 << 20,
        token: token.to_vec(),
    })
    .unwrap()
}

async fn fixture() -> Fixture {
    let server = make(TOKEN, Duration::from_secs(2));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let serving = server.clone();
    let token = stop.clone();
    tokio::spawn(async move {
        serving.serve(listener, token).await.unwrap();
    });
    Fixture {
        client: make(TOKEN, Duration::from_secs(2)),
        server,
        addr,
        stop,
    }
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 + 3) as u8).collect()
}

fn success(completion: Completion, bytes: usize) {
    match completion {
        Completion::Success { bytes: got } => assert_eq!(got, bytes),
        other => panic!("expected success, got {other:?}"),
    }
}

#[tokio::test]
async fn round_trip_around_block_boundary() {
    let fixture = fixture().await;
    let source = pattern(512);
    let remote = fixture
        .server
        .register(source.clone(), Permissions::READWRITE);
    let target = fixture
        .client
        .register(vec![0; 512], Permissions::READWRITE);
    let descriptor = remote.remote(fixture.addr);

    for len in [BLOCK - 1, BLOCK, BLOCK + 1, 300] {
        let handle = fixture
            .client
            .read(descriptor, 17, &target, 33, len)
            .await
            .unwrap();
        success(handle.wait().await, len);
        assert_eq!(target.read(33, len).unwrap(), source[17..17 + len]);
    }

    let data = pattern(256);
    let local = fixture.client.register(data.clone(), Permissions::READ);
    let handle = fixture
        .client
        .write(&local, 0, descriptor, 100, data.len())
        .await
        .unwrap();
    success(handle.wait().await, data.len());
    assert_eq!(remote.read(100, data.len()).unwrap(), data);
}

#[tokio::test]
async fn concurrent_transfers() {
    let fixture = fixture().await;
    let source = pattern(4096);
    let remote = fixture.server.register(source.clone(), Permissions::READ);
    let descriptor = remote.remote(fixture.addr);

    let mut tasks = Vec::new();
    for i in 0..32usize {
        let engine = fixture.client.clone();
        let expected = source[i * 128..(i + 1) * 128].to_vec();
        tasks.push(tokio::spawn(async move {
            let target = engine.register(vec![0; 128], Permissions::READWRITE);
            let handle = engine
                .read(descriptor, (i * 128) as u64, &target, 0, 128)
                .await
                .unwrap();
            success(handle.wait().await, 128);
            assert_eq!(target.read(0, 128).unwrap(), expected);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn batch_submission() {
    let fixture = fixture().await;
    let source = pattern(512);
    let remote = fixture.server.register(source.clone(), Permissions::READ);
    let descriptor = remote.remote(fixture.addr);
    let mut targets = Vec::new();
    let mut transfers = Vec::new();
    for i in 0..4 {
        let target = fixture
            .client
            .register(vec![0; 128], Permissions::READWRITE);
        transfers.push(Transfer {
            direction: transfer::Direction::Read,
            region: target.clone(),
            offset: 0,
            remote: descriptor,
            remote_offset: (i * 128) as u64,
            len: 128,
        });
        targets.push(target);
    }

    let handles = fixture.client.batch(transfers).await.unwrap();
    for handle in handles {
        success(handle.wait().await, 128);
    }
    for (i, target) in targets.iter().enumerate() {
        assert_eq!(target.read(0, 128).unwrap(), source[i * 128..(i + 1) * 128]);
    }
}

#[tokio::test]
async fn rejects_invalid_regions_and_ranges() {
    let fixture = fixture().await;
    let remote = fixture.server.register(pattern(64), Permissions::READ);
    let descriptor = remote.remote(fixture.addr);
    let local = fixture.client.register(vec![0; 64], Permissions::READWRITE);

    assert!(matches!(
        fixture.client.read(descriptor, 0, &local, 32, 64).await,
        Err(Error::Bounds)
    ));
    assert!(matches!(
        fixture
            .client
            .read(descriptor, u64::MAX, &local, 0, 2)
            .await,
        Err(Error::Bounds)
    ));
    assert!(matches!(
        fixture
            .client
            .read(
                descriptor,
                0,
                &fixture.client.register(vec![0; 8], Permissions::READ),
                0,
                8
            )
            .await,
        Err(Error::Permission)
    ));
    assert!(matches!(
        fixture.client.read(descriptor, 0, &local, 0, 1 << 21).await,
        Err(Error::Size)
    ));

    let completion = fixture
        .client
        .read(descriptor, 60, &local, 0, 8)
        .await
        .unwrap()
        .wait()
        .await;
    assert!(matches!(completion, Completion::Failed(Error::Remote(_))));
    assert_eq!(local.read(0, 8).unwrap(), vec![0; 8]);
}

#[tokio::test]
async fn rejects_foreign_and_unknown_regions() {
    let fixture = fixture().await;
    let foreign = make(TOKEN, Duration::from_secs(1)).register(vec![0; 8], Permissions::READWRITE);
    assert!(matches!(
        fixture.client.deregister(&foreign).await,
        Err(Error::Region)
    ));

    let missing = fixture.client.register(vec![0; 8], Permissions::WRITE);
    let local = fixture.client.register(vec![0; 8], Permissions::WRITE);
    let completion = fixture
        .client
        .read(missing.remote(fixture.addr), 0, &local, 0, 8)
        .await
        .unwrap()
        .wait()
        .await;
    assert!(matches!(completion, Completion::Failed(Error::Remote(_))));
}

#[tokio::test]
async fn deregisters_wait_for_inflight_transfer() {
    let (addr, release, server) = holding_server().await;
    let engine = make(TOKEN, Duration::from_secs(2));
    let local = engine.register(vec![0; 32], Permissions::READWRITE);
    let remote = RemoteRegion {
        endpoint: addr,
        id: missing_id(&engine),
    };
    let handle = engine.read(remote, 0, &local, 0, 32).await.unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(100), engine.deregister(&local))
            .await
            .is_err()
    );
    assert!(matches!(local.write(0, &[1]), Err(Error::Closed)));

    release.send(()).unwrap();
    success(handle.wait().await, 32);
    engine.deregister(&local).await.unwrap();
    tokio::join!(engine.deregister(&local), engine.deregister(&local))
        .0
        .unwrap();
    server.abort();
}

fn missing_id(engine: &Engine) -> transfer::RegionId {
    engine.register(Vec::new(), Permissions::READ).id()
}

#[tokio::test]
async fn times_out_without_publishing() {
    let (addr, release, server) = holding_server().await;
    let engine = make(TOKEN, Duration::from_millis(100));
    let local = engine.register(vec![0; 32], Permissions::READWRITE);
    let remote = RemoteRegion {
        endpoint: addr,
        id: missing_id(&engine),
    };
    let handle = engine.read(remote, 0, &local, 0, 32).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    release.send(()).unwrap();
    let completion = handle.wait().await;
    assert!(matches!(completion, Completion::TimedOut));
    assert_eq!(local.read(0, 32).unwrap(), vec![0; 32]);
    server.abort();
}

#[tokio::test]
async fn disconnect_fails_inflight_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        recv(&mut stream).await;
        send(&mut stream, &[1]).await;
        recv(&mut stream).await;
    });

    let engine = make(TOKEN, Duration::from_secs(1));
    let local = engine.register(vec![0; 16], Permissions::READWRITE);
    let remote = RemoteRegion {
        endpoint: addr,
        id: missing_id(&engine),
    };
    let completion = engine
        .read(remote, 0, &local, 0, 16)
        .await
        .unwrap()
        .wait()
        .await;
    assert!(matches!(completion, Completion::Failed(_)));
    server.await.unwrap();
}

#[tokio::test]
async fn rejects_partial_and_wrong_auth() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        recv(&mut stream).await;
        send(&mut stream, &[1]).await;
        let request = recv(&mut stream).await;
        let id = u64::from_be_bytes(request[0..8].try_into().unwrap());
        send(&mut stream, &response(id, 0, &[1, 2, 3])).await;
    });

    let engine = make(TOKEN, Duration::from_secs(1));
    let local = engine.register(vec![0; 8], Permissions::READWRITE);
    let remote = RemoteRegion {
        endpoint: addr,
        id: missing_id(&engine),
    };
    let completion = engine
        .read(remote, 0, &local, 0, 8)
        .await
        .unwrap()
        .wait()
        .await;
    assert!(matches!(completion, Completion::Failed(Error::Protocol(_))));
    assert_eq!(local.read(0, 8).unwrap(), vec![0; 8]);
    server.await.unwrap();

    let fixture = fixture().await;
    let wrong = make(b"wrong", Duration::from_secs(1));
    let target = wrong.register(vec![0; 8], Permissions::WRITE);
    let remote = fixture
        .server
        .register(vec![1; 8], Permissions::READ)
        .remote(fixture.addr);
    let completion = wrong
        .read(remote, 0, &target, 0, 8)
        .await
        .unwrap()
        .wait()
        .await;
    assert!(matches!(completion, Completion::Failed(Error::Auth)));
    fixture.stop.cancel();
    drop(fixture.server);
}

#[tokio::test]
async fn ignores_duplicate_completion() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        recv(&mut stream).await;
        send(&mut stream, &[1]).await;
        let request = recv(&mut stream).await;
        let id = u64::from_be_bytes(request[0..8].try_into().unwrap());
        let len = usize::try_from(u64::from_be_bytes(request[33..41].try_into().unwrap())).unwrap();
        let frame = response(id, 0, &vec![7; len]);
        send(&mut stream, &frame).await;
        send(&mut stream, &frame).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let engine = make(TOKEN, Duration::from_secs(1));
    let local = engine.register(vec![0; 16], Permissions::READWRITE);
    let remote = RemoteRegion {
        endpoint: addr,
        id: missing_id(&engine),
    };
    success(
        engine
            .read(remote, 0, &local, 0, 16)
            .await
            .unwrap()
            .wait()
            .await,
        16,
    );
    assert_eq!(local.read(0, 16).unwrap(), vec![7; 16]);
    server.await.unwrap();
}

async fn holding_server() -> (SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (release, hold) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        recv(&mut stream).await;
        send(&mut stream, &[1]).await;
        let request = recv(&mut stream).await;
        let _ = hold.await;
        let id = u64::from_be_bytes(request[0..8].try_into().unwrap());
        let len = usize::try_from(u64::from_be_bytes(request[33..41].try_into().unwrap())).unwrap();
        send(&mut stream, &response(id, 0, &vec![5; len])).await;
    });
    (addr, release, server)
}

async fn recv(stream: &mut TcpStream) -> Vec<u8> {
    let mut head = [0; 4];
    stream.read_exact(&mut head).await.unwrap();
    let mut frame = vec![0; u32::from_be_bytes(head) as usize];
    stream.read_exact(&mut frame).await.unwrap();
    frame
}

async fn send(stream: &mut TcpStream, frame: &[u8]) {
    stream
        .write_all(&(frame.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(frame).await.unwrap();
    stream.flush().await.unwrap();
}

fn response(id: u64, status: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = id.to_be_bytes().to_vec();
    frame.push(status);
    frame.extend_from_slice(body);
    frame
}

#[test]
fn descriptor() {
    let engine = make(TOKEN, Duration::from_secs(1));
    let endpoint = "[::1]:1234".parse().unwrap();
    let first = engine.register(vec![0; 8], Permissions::READWRITE);
    let second = engine.register(vec![0; 8], Permissions::READWRITE);
    let remote = first.remote(endpoint);
    let decoded: RemoteRegion = remote.to_string().parse().unwrap();
    assert_eq!(decoded.endpoint, endpoint);
    assert_eq!(decoded.id, remote.id);
    assert_ne!(first.id(), second.id());
    assert!("invalid".parse::<RemoteRegion>().is_err());
}

#[tokio::test]
async fn cancellation() {
    let (addr, release, server) = holding_server().await;
    let engine = make(TOKEN, Duration::from_secs(2));
    let local = engine.register(vec![0; 32], Permissions::READWRITE);
    let remote = RemoteRegion {
        endpoint: addr,
        id: missing_id(&engine),
    };
    let handle = engine.read(remote, 0, &local, 0, 32).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.cancel();
    release.send(()).unwrap();
    assert!(matches!(handle.wait().await, Completion::Cancelled));
    assert_eq!(local.read(0, 32).unwrap(), vec![0; 32]);
    server.abort();
}

#[tokio::test]
async fn uncertain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        recv(&mut stream).await;
        send(&mut stream, &[1]).await;
        recv(&mut stream).await;
        // The peer may have committed the write before losing its reply.
    });
    let engine = make(TOKEN, Duration::from_secs(1));
    let local = engine.register(vec![1; 16], Permissions::READ);
    let remote = RemoteRegion {
        endpoint: addr,
        id: missing_id(&engine),
    };
    let result = engine.write(&local, 0, remote, 0, 16).await.unwrap();
    assert!(matches!(
        result.wait().await,
        Completion::Failed(Error::Uncertain { .. })
    ));
    assert!(matches!(
        engine.write(&local, 0, remote, 0, 16).await,
        Err(Error::Quarantined)
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn draining() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, arrived) = oneshot::channel();
    let (release, hold) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        recv(&mut stream).await;
        send(&mut stream, &[1]).await;
        let request = recv(&mut stream).await;
        accepted.send(()).unwrap();
        hold.await.unwrap();
        let id = u64::from_be_bytes(request[..8].try_into().unwrap());
        send(&mut stream, &response(id, 0, &[])).await;
    });
    let engine = make(TOKEN, Duration::from_secs(2));
    let local = engine.register(vec![1; 16], Permissions::READ);
    let remote = RemoteRegion {
        endpoint: addr,
        id: missing_id(&engine),
    };
    let handle = engine.write(&local, 0, remote, 0, 16).await.unwrap();
    arrived.await.unwrap();
    handle.cancel();
    let completion = tokio::spawn(handle.wait());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), engine.deregister(&local))
            .await
            .is_err()
    );
    assert!(!completion.is_finished());
    release.send(()).unwrap();
    assert!(matches!(completion.await.unwrap(), Completion::Cancelled));
    engine.deregister(&local).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn admission() {
    let engine = Engine::new(Config {
        limit: 1,
        timeout: Duration::from_secs(2),
        frame: 4096,
        token: TOKEN.to_vec(),
    })
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let server = tokio::spawn(async move { engine.serve(listener, token).await });
    let mut first = TcpStream::connect(addr).await.unwrap();
    let mut handshake = b"KVTE\x02".to_vec();
    handshake.extend_from_slice(TOKEN);
    send(&mut first, &handshake).await;
    assert_eq!(recv(&mut first).await, vec![1]);
    // Hold the only receive permit with an incomplete payload.
    first.write_all(&1024u32.to_be_bytes()).await.unwrap();
    let mut second = TcpStream::connect(addr).await.unwrap();
    let mut byte = [0];
    let read = tokio::time::timeout(Duration::from_secs(1), second.read(&mut byte))
        .await
        .unwrap();
    assert!(matches!(read, Ok(0) | Err(_)));
    stop.cancel();
    server.await.unwrap().unwrap();
    let read = first.read(&mut byte).await;
    assert!(matches!(read, Ok(0) | Err(_)));
}
