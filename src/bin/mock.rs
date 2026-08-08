#[cfg(feature = "mock")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use axum::{
        Json, Router,
        routing::{get, post},
    };
    use rmpv::Value;
    use std::sync::{Arc, Mutex};

    type Replay = Arc<Mutex<Vec<(u64, Vec<u8>)>>>;

    #[derive(Clone)]
    struct MockState {
        load: Arc<Mutex<Load>>,
        chats: Arc<Mutex<Vec<serde_json::Value>>>,
        replay: Replay,
        seq: Arc<Mutex<u64>>,
        publish: std::sync::mpsc::Sender<(u64, Vec<u8>)>,
    }

    #[derive(Clone, Default, serde::Deserialize)]
    struct Load {
        running: u64,
        waiting: u64,
        kv_usage: f64,
    }

    fn msgpack(v: &serde_json::Value) -> Value {
        match v {
            serde_json::Value::String(s) => Value::String(s.as_str().into()),
            serde_json::Value::Number(n) => Value::Integer(n.as_i64().unwrap_or_default().into()),
            _ => Value::Nil,
        }
    }

    fn array(v: &serde_json::Value) -> Vec<Value> {
        v.as_array()
            .map(|a| a.iter().map(msgpack).collect())
            .unwrap_or_default()
    }

    fn record(s: &MockState, batch: Value) -> Json<serde_json::Value> {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &batch).unwrap();
        let mut seq = s.seq.lock().unwrap();
        let current = *seq;
        *seq += 1;
        s.replay.lock().unwrap().push((current, buf.clone()));
        let _ = s.publish.send((current, buf));
        Json(serde_json::json!({ "sequence": current }))
    }

    let (publish, events) = std::sync::mpsc::channel();
    let state = MockState {
        load: Arc::new(Mutex::new(Load::default())),
        chats: Arc::new(Mutex::new(Vec::new())),
        replay: Arc::new(Mutex::new(Vec::new())),
        seq: Arc::new(Mutex::new(0)),
        publish,
    };

    let s = state.clone();
    std::thread::spawn(move || {
        let ctx = zmq::Context::new();
        let pub_socket = ctx.socket(zmq::PUB).unwrap();
        pub_socket.bind("tcp://*:5557").unwrap();
        let router_socket = ctx.socket(zmq::ROUTER).unwrap();
        router_socket.bind("tcp://*:5558").unwrap();

        let empty: &[u8] = b"";
        let topic: &[u8] = b"kv-events";
        loop {
            while let Ok((seq, payload)) = events.try_recv() {
                let _ = pub_socket
                    .send_multipart([topic, seq.to_be_bytes().as_slice(), payload.as_slice()], 0);
            }
            if !router_socket.poll(zmq::POLLIN, 100).is_ok_and(|r| r > 0) {
                continue;
            }
            let Ok(frames) = router_socket.recv_multipart(0) else {
                continue;
            };
            if frames.len() < 3 {
                continue;
            }
            let from = u64::from_be_bytes(frames[2][..8].try_into().unwrap_or([0; 8]));
            for (seq, payload) in s.replay.lock().unwrap().iter() {
                if *seq >= from {
                    let _ = router_socket.send_multipart(
                        [
                            frames[0].as_slice(),
                            empty,
                            topic,
                            seq.to_be_bytes().as_slice(),
                            payload.as_slice(),
                        ],
                        0,
                    );
                }
            }
            let _ = router_socket.send_multipart(
                [
                    frames[0].as_slice(),
                    empty,
                    empty,
                    (-1i64).to_be_bytes().as_slice(),
                    empty,
                ],
                0,
            );
        }
    });

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route(
            "/v1/models",
            get(|| async {
                Json(serde_json::json!({
                    "object": "list",
                    "data": [{"id": "fixed", "root": "abc123", "owned_by": "mock"}]
                }))
            }),
        )
        .route("/metrics", get({
            let s = state.clone();
            move || async move {
                let load = s.load.lock().unwrap();
                format!(
                    "vllm:num_requests_running{{model_name=\"fixed\"}} {}\nvllm:num_requests_waiting{{model_name=\"fixed\"}} {}\nvllm:kv_cache_usage_perc{{model_name=\"fixed\"}} {}\n",
                    load.running, load.waiting, load.kv_usage
                )
            }
        }))
        .route("/v1/chat/completions/render", post({
            |Json(_): Json<serde_json::Value>| async move {
                Json(serde_json::json!({
                    "request_id": "mock-r1",
                    "token_ids": [1, 2, 3, 4],
                    "sampling_params": {}
                }))
            }
        }))
        .route("/v1/chat/completions", post({
            let s = state.clone();
            move |Json(body): Json<serde_json::Value>| {
                let s = s.clone();
                async move {
                    s.chats.lock().unwrap().push(body);
                    Json(serde_json::json!({
                        "choices": [{"message": {"role": "assistant", "content": "mock response"}}]
                    }))
                }
            }
        }))
        .route("/control/load", post({
            let s = state.clone();
            move |Json(load): Json<Load>| {
                let s = s.clone();
                async move {
                    *s.load.lock().unwrap() = load;
                    "ok"
                }
            }
        }))
        .route("/control/store", post({
            let s = state.clone();
            move |Json(body): Json<serde_json::Value>| {
                let s = s.clone();
                async move {
                    let event = Value::Map(vec![
                        (Value::String("type".into()), Value::String("BlockStored".into())),
                        (
                            Value::String("block_hashes".into()),
                            Value::Array(array(&body["block_hashes"])),
                        ),
                        (
                            Value::String("token_ids".into()),
                            Value::Array(array(&body["token_ids"])),
                        ),
                        (
                            Value::String("block_size".into()),
                            Value::Integer(body["block_size"].as_i64().unwrap_or(4).into()),
                        ),
                    ]);
                    record(&s, Value::Array(vec![Value::F64(1.0), Value::Array(vec![event])]))
                }
            }
        }))
        .route("/control/remove", post({
            let s = state.clone();
            move |Json(body): Json<serde_json::Value>| {
                let s = s.clone();
                async move {
                    let event = Value::Map(vec![
                        (Value::String("type".into()), Value::String("BlockRemoved".into())),
                        (
                            Value::String("block_hashes".into()),
                            Value::Array(array(&body["block_hashes"])),
                        ),
                    ]);
                    record(&s, Value::Array(vec![Value::F64(1.0), Value::Array(vec![event])]))
                }
            }
        }))
        .route("/control/clear", post({
            let s = state.clone();
            move |Json(_): Json<serde_json::Value>| {
                let s = s.clone();
                async move {
                    let event = Value::Map(vec![(
                        Value::String("type".into()),
                        Value::String("AllBlocksCleared".into()),
                    )]);
                    record(&s, Value::Array(vec![Value::F64(1.0), Value::Array(vec![event])]))
                }
            }
        }))
        .route("/control/gap", post({
            let s = state.clone();
            move |Json(body): Json<serde_json::Value>| {
                let s = s.clone();
                async move {
                    let skip = body["skip"].as_u64().unwrap_or(1);
                    let retain = body["retain"].as_bool().unwrap_or(true);
                    let mut seq = s.seq.lock().unwrap();
                    *seq += skip;
                    if !retain {
                        s.replay.lock().unwrap().clear();
                    }
                    Json(serde_json::json!({ "skipped_to": *seq }))
                }
            }
        }))
        .route("/control/last", get({
            let s = state;
            move || async move {
                let chats = s.chats.lock().unwrap();
                Json(chats.last().cloned().unwrap_or_default())
            }
        }));

    let listener = tokio::net::TcpListener::bind(
        std::env::var("MOCK_LISTEN").unwrap_or_else(|_| "0.0.0.0:8000".into()),
    )
    .await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(not(feature = "mock"))]
fn main() {}
