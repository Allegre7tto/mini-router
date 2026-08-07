use rmpv::Value;
use router::kv::events::{BlockHash, CacheEvent, DecodeError, decode};

fn encode_event(batch: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, batch).unwrap();
    buf
}

fn frames(topic: &[u8], seq: u64, payload: &[u8]) -> Vec<Vec<u8>> {
    let seq_bytes = seq.to_be_bytes();
    vec![topic.to_vec(), seq_bytes.to_vec(), payload.to_vec()]
}

#[allow(dead_code)]
fn make_store_payload(tokens: &[u32], block_size: usize) -> Value {
    let hashes: Vec<Value> = (0..tokens.len() / block_size)
        .map(|i| Value::Integer((i as u64).into()))
        .collect();
    Value::Array(vec![
        Value::F64(1.0),
        Value::Array(vec![Value::Map(vec![
            (
                Value::String("type".into()),
                Value::String("BlockStored".into()),
            ),
            (Value::String("block_hashes".into()), Value::Array(hashes)),
            (
                Value::String("token_ids".into()),
                Value::Array(tokens.iter().map(|t| Value::Integer((*t).into())).collect()),
            ),
            (
                Value::String("block_size".into()),
                Value::Integer((block_size as i64).into()),
            ),
        ])]),
    ])
}

#[allow(dead_code)]
fn seq_to_msgpack(_seq: u64) -> Value {
    let payload = make_store_payload(&[1, 2, 3, 4, 5, 6, 7, 8], 4);
    Value::Array(payload.as_array().unwrap().clone())
}

fn store_batch() -> Vec<u8> {
    let batch = Value::Array(vec![
        Value::F64(1.0),
        Value::Array(vec![Value::Map(vec![
            (
                Value::String("type".into()),
                Value::String("BlockStored".into()),
            ),
            (
                Value::String("block_hashes".into()),
                Value::Array(vec![
                    Value::Binary(b"a".to_vec()),
                    Value::Binary(b"b".to_vec()),
                ]),
            ),
            (
                Value::String("token_ids".into()),
                Value::Array(vec![
                    Value::Integer(1.into()),
                    Value::Integer(2.into()),
                    Value::Integer(3.into()),
                    Value::Integer(4.into()),
                    Value::Integer(5.into()),
                    Value::Integer(6.into()),
                    Value::Integer(7.into()),
                    Value::Integer(8.into()),
                ]),
            ),
            (Value::String("block_size".into()), Value::Integer(4.into())),
            (Value::String("medium".into()), Value::String("GPU".into())),
        ])]),
    ]);
    encode_event(&batch)
}

fn remove_batch() -> Vec<u8> {
    let batch = Value::Array(vec![
        Value::F64(2.0),
        Value::Array(vec![Value::Map(vec![
            (
                Value::String("type".into()),
                Value::String("BlockRemoved".into()),
            ),
            (
                Value::String("block_hashes".into()),
                Value::Array(vec![Value::Binary(b"b".to_vec())]),
            ),
            (Value::String("medium".into()), Value::String("GPU".into())),
        ])]),
    ]);
    encode_event(&batch)
}

#[test]
fn decode_store_event() {
    let payload = store_batch();
    let f = frames(b"kv-events", 42, &payload);
    let frame_refs: Vec<&[u8]> = f.iter().map(|v| v.as_slice()).collect();
    let result = decode(&frame_refs).unwrap();
    assert_eq!(result.sequence, 42);
    match &result.batch.events[0] {
        CacheEvent::Stored {
            hashes,
            parent,
            tokens,
            size,
            group,
        } => {
            assert_eq!(hashes.len(), 2);
            assert!(matches!(&hashes[0], BlockHash::Bytes(b) if b == b"a"));
            assert!(matches!(&hashes[1], BlockHash::Bytes(b) if b == b"b"));
            assert!(parent.is_none());
            assert_eq!(tokens.as_slice(), &[1, 2, 3, 4, 5, 6, 7, 8]);
            assert_eq!(*size, 4);
            assert_eq!(*group, 0);
        }
        _ => panic!("expected Stored event"),
    }
}

#[test]
fn decode_remove_event() {
    let payload = remove_batch();
    let f = frames(b"kv-events", 7, &payload);
    let frame_refs: Vec<&[u8]> = f.iter().map(|v| v.as_slice()).collect();
    let result = decode(&frame_refs).unwrap();
    assert_eq!(result.sequence, 7);
    match &result.batch.events[0] {
        CacheEvent::Removed { hashes, group } => {
            assert_eq!(hashes.len(), 1);
            assert!(matches!(&hashes[0], BlockHash::Bytes(b) if b == b"b"));
            assert_eq!(*group, 0);
        }
        _ => panic!("expected Removed event"),
    }
}

#[test]
fn decode_rejects_wrong_frame_count() {
    let frames_refs: Vec<&[u8]> = vec![b"topic", b"seq"];
    let result = decode(&frames_refs);
    assert!(matches!(result, Err(DecodeError::FrameCount)));
}

#[test]
fn decode_rejects_negative_tokens() {
    let batch = Value::Array(vec![
        Value::F64(1.0),
        Value::Array(vec![Value::Map(vec![
            (
                Value::String("type".into()),
                Value::String("BlockStored".into()),
            ),
            (
                Value::String("block_hashes".into()),
                Value::Array(vec![Value::Binary(b"a".to_vec())]),
            ),
            (
                Value::String("token_ids".into()),
                Value::Array(vec![Value::Integer((-1).into())]),
            ),
            (Value::String("block_size".into()), Value::Integer(1.into())),
        ])]),
    ]);
    let payload = encode_event(&batch);
    let f = frames(b"kv-events", 1, &payload);
    let frame_refs: Vec<&[u8]> = f.iter().map(|v| v.as_slice()).collect();
    let result = decode(&frame_refs);
    assert!(result.is_err());
}

#[test]
fn decode_rejects_mismatched_tokens_hashes() {
    let map = vec![
        (
            Value::String("type".into()),
            Value::String("BlockStored".into()),
        ),
        (
            Value::String("block_hashes".into()),
            Value::Array(vec![
                Value::Binary(b"a".to_vec()),
                Value::Binary(b"b".to_vec()),
            ]),
        ),
        (
            Value::String("token_ids".into()),
            Value::Array(vec![Value::Integer(1.into())]),
        ),
        (Value::String("block_size".into()), Value::Integer(4.into())),
    ];
    let batch = Value::Array(vec![Value::F64(1.0), Value::Array(vec![Value::Map(map)])]);
    let payload = encode_event(&batch);
    let f = frames(b"kv-events", 1, &payload);
    let frame_refs: Vec<&[u8]> = f.iter().map(|v| v.as_slice()).collect();
    let result = decode(&frame_refs);
    assert!(matches!(result, Err(DecodeError::TokenHashMismatch(..))));
}

#[test]
fn decode_cleared_event() {
    let batch = Value::Array(vec![
        Value::F64(3.0),
        Value::Array(vec![Value::Map(vec![(
            Value::String("type".into()),
            Value::String("AllBlocksCleared".into()),
        )])]),
    ]);
    let payload = encode_event(&batch);
    let f = frames(b"kv-events", 99, &payload);
    let frame_refs: Vec<&[u8]> = f.iter().map(|v| v.as_slice()).collect();
    let result = decode(&frame_refs).unwrap();
    assert_eq!(result.sequence, 99);
    assert!(matches!(&result.batch.events[0], CacheEvent::Cleared));
}

#[test]
fn decode_integer_block_hash() {
    let map = vec![
        (
            Value::String("type".into()),
            Value::String("BlockStored".into()),
        ),
        (
            Value::String("block_hashes".into()),
            Value::Array(vec![Value::Integer(42.into())]),
        ),
        (
            Value::String("token_ids".into()),
            Value::Array(vec![
                Value::Integer(1.into()),
                Value::Integer(2.into()),
                Value::Integer(3.into()),
                Value::Integer(4.into()),
            ]),
        ),
        (Value::String("block_size".into()), Value::Integer(4.into())),
    ];
    let batch = Value::Array(vec![Value::F64(1.0), Value::Array(vec![Value::Map(map)])]);
    let payload = encode_event(&batch);
    let f = frames(b"kv-events", 1, &payload);
    let frame_refs: Vec<&[u8]> = f.iter().map(|v| v.as_slice()).collect();
    let result = decode(&frame_refs).unwrap();
    match &result.batch.events[0] {
        CacheEvent::Stored { hashes, .. } => {
            assert!(matches!(&hashes[0], BlockHash::Integer(42)));
        }
        _ => panic!("expected Stored"),
    }
}
