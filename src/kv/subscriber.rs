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
        let addr = self.endpoint.0.ip().to_string();
        let topic = self.topic.clone();

        std::thread::spawn(move || {
            let ctx = zmq::Context::new();
            let sub = match ctx.socket(zmq::SUB) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("failed to create SUB socket: {e}");
                    return;
                }
            };

            if let Err(e) = sub.connect(&format!("tcp://{addr}:{kv_port}")) {
                tracing::error!("failed to connect SUB to {addr}:{kv_port}: {e}");
                return;
            }
            if let Err(e) = sub.set_subscribe(topic.as_bytes()) {
                tracing::error!("failed to subscribe: {e}");
                return;
            }

            let mut expected: u64 = 0;

            loop {
                if cancel.is_cancelled() {
                    let _ = sub.set_linger(0);
                    break;
                }

                match sub.poll(zmq::POLLIN, POLL_TIMEOUT_MS) {
                    Ok(0) => continue,
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!("zmq poll error: {e}");
                        continue;
                    }
                }

                let frames = match sub.recv_multipart(0) {
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

                let refs: Vec<&[u8]> = frames.iter().map(|f| f.as_slice()).collect();
                let batch = match decode(&refs) {
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
                    let replay = zmq::Context::new();
                    if let Ok(req) = replay.socket(zmq::REQ)
                        && req.connect(&format!("tcp://{addr}:{replay_port}")).is_ok()
                        && req.set_rcvtimeo(REPLAY_TIMEOUT_MS).is_ok()
                        && req.set_sndtimeo(REPLAY_TIMEOUT_MS).is_ok()
                    {
                        let _ = req.send(expected.to_be_bytes().as_slice(), 0);
                        while let Ok(frames) = req.recv_multipart(0) {
                            if frames.len() < 4 {
                                break;
                            }
                            if frames.len() >= 5 && frames[2].is_empty() {
                                if frames[3].len() >= 8
                                    && i64::from_be_bytes(
                                        frames[3][..8].try_into().unwrap_or([0; 8]),
                                    ) == -1
                                {
                                    break;
                                }
                                continue;
                            }
                            if frames.len() == 5 {
                                let refs: Vec<&[u8]> = vec![&frames[2], &frames[3], &frames[4]];
                                if let Ok(batch) = decode(&refs) {
                                    let _ = sender.blocking_send(batch);
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
