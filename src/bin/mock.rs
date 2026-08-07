#[cfg(feature = "mock")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use axum::{
        Json, Router,
        extract::State,
        routing::{get, post},
    };
    use rmpv::Value;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct MockState {
        load: Arc<Mutex<LoadData>>,
        chat_requests: Arc<Mutex<Vec<serde_json::Value>>>,
        replay: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
        seq: Arc<Mutex<u64>>,
    }

    #[derive(Clone, Default)]
    struct LoadData {
        running: u64,
        waiting: u64,
        kv_usage: f64,
    }

    let state = MockState::default();
    let state_clone = state.clone();

    std::thread::spawn(move || {
        let ctx = zmq::Context::new();
        let pub_socket = ctx.socket(zmq::PUB).unwrap();
        pub_socket.bind("tcp://*:5557").unwrap();

        let router_socket = ctx.socket(zmq::ROUTER).unwrap();
        router_socket.bind("tcp://*:5558").unwrap();

        let topic = b"kv-events";
        loop {
            if router_socket.poll(zmq::POLLIN, 100).is_ok_and(|r| r > 0) {
                if let Ok(frames) = router_socket.recv_multipart(0) {
                    if frames.len() >= 3 {
                        let seq = u64::from_be_bytes(frames[2][..8].try_into().unwrap_or([0; 8]));
                        let replay = state_clone.replay.lock().unwrap();
                        for (s, payload) in replay.iter() {
                            if *s >= seq {
                                let seq_bytes = s.to_be_bytes();
                                let _ = router_socket.send_multipart(
                                    &[&frames[0], b"", topic, &seq_bytes, payload],
                                    0,
                                );
                            }
                        }
                        let _ = router_socket.send_multipart(
                            &[&frames[0], b"", b"", &(-1i64).to_be_bytes(), b""],
                            0,
                        );
                    }
                }
            }
        }
    });

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
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
            |Json(body): Json<serde_json::Value>| async move {
                Json(serde_json::json!({
                    "request_id": "mock-r1",
                    "token_ids": [1, 2, 3, 4],
                    "sampling_params": {}
                }))
            }
        }))
        .route("/v1/chat/completions", post({
            let s = state.clone();
            |Json(body): Json<serde_json::Value>| async move {
                s.chat_requests.lock().unwrap().push(body);
                Json(serde_json::json!({
                    "choices": [{"message": {"role": "assistant", "content": "mock response"}}]
                }))
            }
        }))
        .route("/control/load", post({
            let s = state.clone();
            |Json(load): Json<LoadData>| async move {
                *s.load.lock().unwrap() = load;
                "ok"
            }
        }))
        .route("/control/store", post({
            let s = state.clone();
            |Json(body): Json<serde_json::Value>| async move {
                let batch = Value::Array(vec![
                    Value::F64(1.0),
                    Value::Array(vec![Value::Map(vec![
                        (Value::String("type".into()), Value::String("BlockStored".into())),
                        (Value::String("block_hashes".into()), Value::Array(
                            body["block_hashes"].as_array().cloned().unwrap_or_default()
                        )),
                        (Value::String("token_ids".into()), Value::Array(
                            body["token_ids"].as_array().cloned().unwrap_or_default()
                        )),
                        (Value::String("block_size".into()), Value::Integer(body.get("block_size").and_then(|v| v.as_i64()).unwrap_or(4).into())),
                    ])]),
                ]);
                let mut buf = Vec::new();
                rmpv::encode::write_value(&mut buf, &batch).unwrap();
                let mut seq = s.seq.lock().unwrap();
                let current = *seq;
                *seq += 1;
                s.replay.lock().unwrap().push((current, buf.clone()));
                Json(serde_json::json!({"sequence": current}))
            }
        }))
        .route("/control/remove", post({
            let s = state.clone();
            |Json(body): Json<serde_json::Value>| async move {
                let batch = Value::Array(vec![
                    Value::F64(1.0),
                    Value::Array(vec![Value::Map(vec![
                        (Value::String("type".into()), Value::String("BlockRemoved".into())),
                        (Value::String("block_hashes".into()), Value::Array(
                            body["block_hashes"].as_array().cloned().unwrap_or_default()
                        )),
                    ])]),
                ]);
                let mut buf = Vec::new();
                rmpv::encode::write_value(&mut buf, &batch).unwrap();
                let mut seq = s.seq.lock().unwrap();
                let current = *seq;
                *seq += 1;
                s.replay.lock().unwrap().push((current, buf.clone()));
                Json(serde_json::json!({"sequence": current}))
            }
        }))
        .route("/control/clear", post({
            let s = state.clone();
            |Json(_body): Json<serde_json::Value>| async move {
                let batch = Value::Array(vec![
                    Value::F64(1.0),
                    Value::Array(vec![Value::Map(vec![(
                        Value::String("type".into()),
                        Value::String("AllBlocksCleared".into()),
                    )])]),
                ]);
                let mut buf = Vec::new();
                rmpv::encode::write_value(&mut buf, &batch).unwrap();
                let mut seq = s.seq.lock().unwrap();
                let current = *seq;
                *seq += 1;
                s.replay.lock().unwrap().push((current, buf.clone()));
                Json(serde_json::json!({"sequence": current}))
            }
        }))
        .route("/control/gap", post({
            let s = state.clone();
            |Json(body): Json<serde_json::Value>| async move {
                let skip = body["skip"].as_u64().unwrap_or(1);
                let retain = body["retain"].as_bool().unwrap_or(true);
                let mut seq = s.seq.lock().unwrap();
                *seq += skip;
                if !retain {
                    s.replay.lock().unwrap().clear();
                }
                Json(serde_json::json!({"skipped_to": *seq}))
            }
        }))
        .route("/control/last", get({
            let s = state;
            move || async move {
                let reqs = s.chat_requests.lock().unwrap();
                Json(reqs.last().cloned().unwrap_or_default())
            }
        }));

    let addr = "0.0.0.0:8000".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(not(feature = "mock"))]
fn main() {}
