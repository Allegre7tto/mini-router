use crate::kv::events::{EventBatch, decode};
use crate::types::WorkerKey;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const POLL_TIMEOUT_MS: i64 = 1000;
const REPLAY_TIMEOUT_MS: i32 = 5000;

#[derive(Debug)]
pub enum Event {
    Batch(EventBatch),
    Gap,
    Replayed,
    Reset,
    Duplicate,
}

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
        sender: mpsc::Sender<(WorkerKey, Event)>,
        cancel: CancellationToken,
    ) {
        let addr = self.endpoint.0.ip().to_string();
        let topic = self.topic.clone();
        let worker = self.endpoint.clone();

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
                    let _ = sender.blocking_send((worker.clone(), Event::Duplicate));
                    continue;
                }

                if batch.sequence > expected {
                    let _ = sender.blocking_send((worker.clone(), Event::Gap));
                    let mut synced = false;
                    if let Ok(req) = ctx.socket(zmq::REQ)
                        && req.connect(&format!("tcp://{addr}:{replay_port}")).is_ok()
                        && req.set_rcvtimeo(REPLAY_TIMEOUT_MS).is_ok()
                        && req.set_sndtimeo(REPLAY_TIMEOUT_MS).is_ok()
                        && req.send(expected.to_be_bytes().as_slice(), 0).is_ok()
                    {
                        while let Ok(frames) = req.recv_multipart(0) {
                            if frames.len() != 3 || frames[0].is_empty() {
                                break;
                            }
                            let refs: Vec<&[u8]> = frames.iter().map(|f| f.as_slice()).collect();
                            let Ok(replayed) = decode(&refs) else {
                                break;
                            };
                            if replayed.sequence != expected {
                                break;
                            }
                            let _ = sender
                                .blocking_send((worker.clone(), Event::Batch(replayed.batch)));
                            expected += 1;
                        }
                        synced = expected == batch.sequence;
                    }
                    let event = if synced {
                        Event::Replayed
                    } else {
                        Event::Reset
                    };
                    let _ = sender.blocking_send((worker.clone(), event));
                }

                let _ = sender.blocking_send((worker.clone(), Event::Batch(batch.batch)));
                expected = batch.sequence + 1;
            }
        });
    }
}
