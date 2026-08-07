use crate::kv::events::{SequencedBatch, decode};
use crate::types::WorkerKey;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum SubscriberError {
    #[error("zmq error: {0}")]
    Zmq(#[from] zmq::Error),
    #[error("decode error: {0}")]
    Decode(#[from] super::events::DecodeError),
    #[error("gap: expected {expected}, available {available}")]
    Gap { expected: u64, available: u64 },
}

const POLL_TIMEOUT_MS: i64 = 1000;
const REPLAY_TIMEOUT_MS: i32 = 5000;

pub struct Subscriber {
    endpoint: WorkerKey,
    topic: String,
}

impl Subscriber {
    pub fn new(endpoint: WorkerKey, topic: String) -> Self {
        Subscriber { endpoint, topic }
    }

    pub fn run(
        &self,
        kv_port: u16,
        replay_port: u16,
        sender: mpsc::Sender<SequencedBatch>,
        cancel: CancellationToken,
    ) {
        let endpoint = self.endpoint.clone();
        let topic = self.topic.clone();
        let addr = endpoint.0.ip().to_string();

        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let pub_url = format!("tcp://{addr}:{kv_port}");
            let replay_url = format!("tcp://{addr}:{replay_port}");
            let topic_bytes = topic.as_bytes();

            let sub_socket = match ctx.socket(zmq::SUB) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("failed to create SUB socket: {e}");
                    return;
                }
            };

            if let Err(e) = sub_socket.connect(&pub_url) {
                tracing::error!("failed to connect SUB to {pub_url}: {e}");
                return;
            }
            if let Err(e) = sub_socket.set_subscribe(topic_bytes) {
                tracing::error!("failed to subscribe: {e}");
                return;
            }

            let mut expected: u64 = 0;

            loop {
                if cancel.is_cancelled() {
                    let _ = sub_socket.set_linger(0);
                    break;
                }

                match sub_socket.poll(zmq::POLLIN, POLL_TIMEOUT_MS) {
                    Ok(0) => continue,
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!("zmq poll error: {e}");
                        continue;
                    }
                }

                let frames = match sub_socket.recv_multipart(0) {
                    Ok(f) => f,
                    Err(e) => {
                        if cancel.is_cancelled() {
                            break;
                        }
                        tracing::warn!("zmq recv error: {e}");
                        continue;
                    }
                };

                if frames.len() != 3 {
                    continue;
                }

                let frame_refs: Vec<&[u8]> = frames.iter().map(|f| f.as_slice()).collect();

                let batch = match decode(&frame_refs) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!("decode error: {e}");
                        continue;
                    }
                };

                if batch.sequence < expected {
                    continue;
                }

                if batch.sequence > expected {
                    let replay_ctx = zmq::Context::new();
                    if let Ok(req) = replay_ctx.socket(zmq::REQ)
                        && req.connect(&replay_url).is_ok()
                        && req.set_rcvtimeo(REPLAY_TIMEOUT_MS).is_ok()
                        && req.set_sndtimeo(REPLAY_TIMEOUT_MS).is_ok()
                    {
                        let req_bytes = expected.to_be_bytes();
                        let _ = req.send(&req_bytes[..], 0);
                        while let Ok(replay_frames) = req.recv_multipart(0) {
                            if replay_frames.len() < 4 {
                                break;
                            }
                            if replay_frames.len() >= 5 && replay_frames[2].is_empty() {
                                if replay_frames[3].len() >= 8 {
                                    let end_seq = i64::from_be_bytes(
                                        replay_frames[3][..8].try_into().unwrap_or([0; 8]),
                                    );
                                    if end_seq == -1 {
                                        break;
                                    }
                                }
                                continue;
                            }
                            if replay_frames.len() == 5 {
                                let r_refs: Vec<&[u8]> =
                                    vec![&replay_frames[2], &replay_frames[3], &replay_frames[4]];
                                if let Ok(r_batch) = decode(&r_refs) {
                                    let _ = sender.blocking_send(r_batch);
                                }
                            }
                        }
                    }
                    continue;
                }

                let _ = sender.blocking_send(batch);
                expected += 1;
            }
        });
    }
}
