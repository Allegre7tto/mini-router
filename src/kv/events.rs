use rmpv::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockHash {
    Bytes(Vec<u8>),
    Integer(u64),
}

#[derive(Debug, Clone)]
pub enum CacheEvent {
    Stored {
        hashes: Vec<BlockHash>,
        parent: Option<BlockHash>,
        tokens: Vec<u32>,
        size: usize,
        group: u32,
    },
    Removed {
        hashes: Vec<BlockHash>,
        group: u32,
    },
    Cleared,
}

#[derive(Debug, Clone)]
pub struct EventBatch {
    pub ts: f64,
    pub events: Vec<CacheEvent>,
    pub data_parallel_rank: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct SequencedBatch {
    pub sequence: u64,
    pub batch: EventBatch,
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("expected exactly 3 frames")]
    FrameCount,
    #[error("invalid sequence number: {0}")]
    Sequence(String),
    #[error("msgpack decode error: {0}")]
    MsgPack(String),
    #[error("invalid event type: {0}")]
    EventType(String),
    #[error("token count mismatch: {0} tokens for {1} hashes at block size {2}")]
    TokenHashMismatch(usize, usize, usize),
    #[error("invalid parent hash type")]
    ParentHashType,
}

pub fn decode(frames: &[&[u8]]) -> Result<SequencedBatch, DecodeError> {
    if frames.len() != 3 {
        return Err(DecodeError::FrameCount);
    }

    let seq_bytes: [u8; 8] = frames[1]
        .try_into()
        .map_err(|_| DecodeError::Sequence("wrong length".into()))?;
    let sequence = u64::from_be_bytes(seq_bytes);

    let mut cursor = frames[2];
    let value =
        rmpv::decode::read_value(&mut cursor).map_err(|e| DecodeError::MsgPack(e.to_string()))?;

    let batch = decode_batch(&value)?;
    Ok(SequencedBatch { sequence, batch })
}

fn decode_batch(value: &Value) -> Result<EventBatch, DecodeError> {
    let arr = value
        .as_array()
        .ok_or_else(|| DecodeError::MsgPack("expected array for EventBatch".into()))?;

    if arr.len() < 2 {
        return Err(DecodeError::MsgPack("EventBatch array too short".into()));
    }

    let ts = arr[0]
        .as_f64()
        .ok_or_else(|| DecodeError::MsgPack("expected f64 ts".into()))?;

    let events_arr = arr[1]
        .as_array()
        .ok_or_else(|| DecodeError::MsgPack("expected array for events".into()))?;

    let mut events = Vec::with_capacity(events_arr.len());
    for ev in events_arr {
        events.push(decode_event(ev)?);
    }

    let data_parallel_rank = if arr.len() >= 3 {
        match &arr[2] {
            Value::Nil => None,
            v => Some(
                v.as_u64()
                    .ok_or_else(|| DecodeError::MsgPack("expected u64 data_parallel_rank".into()))?
                    as u32,
            ),
        }
    } else {
        None
    };

    Ok(EventBatch {
        ts,
        events,
        data_parallel_rank,
    })
}

fn decode_event(value: &Value) -> Result<CacheEvent, DecodeError> {
    let map = value
        .as_map()
        .ok_or_else(|| DecodeError::MsgPack("expected map for event".into()))?;

    let event_type = map
        .iter()
        .find(|(k, _)| k.as_str() == Some("type"))
        .and_then(|(_, v)| v.as_str())
        .ok_or_else(|| DecodeError::EventType("missing type field".into()))?;

    match event_type {
        "BlockStored" => decode_store(map),
        "BlockRemoved" => decode_remove(map),
        "AllBlocksCleared" => Ok(CacheEvent::Cleared),
        other => Err(DecodeError::EventType(other.to_string())),
    }
}

fn find_field<'a>(map: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    map.iter()
        .find(|(k, _)| k.as_str() == Some(key))
        .map(|(_, v)| v)
}

fn decode_hashes(value: &Value) -> Result<Vec<BlockHash>, DecodeError> {
    let arr = value
        .as_array()
        .ok_or_else(|| DecodeError::MsgPack("expected array for hashes".into()))?;
    arr.iter().map(decode_hash).collect()
}

fn decode_hash(value: &Value) -> Result<BlockHash, DecodeError> {
    if let Some(bytes) = value.as_slice() {
        Ok(BlockHash::Bytes(bytes.to_vec()))
    } else if let Some(n) = value.as_u64() {
        Ok(BlockHash::Integer(n))
    } else {
        Err(DecodeError::MsgPack("invalid block hash type".into()))
    }
}

fn decode_store(map: &[(Value, Value)]) -> Result<CacheEvent, DecodeError> {
    let hashes = find_field(map, "block_hashes")
        .map(decode_hashes)
        .unwrap_or(Ok(Vec::new()))?;

    let parent = find_field(map, "parent_block_hash")
        .map(decode_hash)
        .transpose()?;

    let tokens: Vec<u32> = find_field(map, "token_ids")
        .and_then(|v| v.as_array())
        .and_then(|arr| {
            arr.iter()
                .map(|v| v.as_u64().and_then(|n| u32::try_from(n).ok()))
                .collect::<Option<Vec<u32>>>()
        })
        .unwrap_or_default();

    let block_size = find_field(map, "block_size")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as usize;

    let group = find_field(map, "group_idx")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;

    if block_size > 0 && tokens.len() != hashes.len() * block_size {
        return Err(DecodeError::TokenHashMismatch(
            tokens.len(),
            hashes.len(),
            block_size,
        ));
    }

    Ok(CacheEvent::Stored {
        hashes,
        parent,
        tokens,
        size: block_size,
        group,
    })
}

fn decode_remove(map: &[(Value, Value)]) -> Result<CacheEvent, DecodeError> {
    let hashes = find_field(map, "block_hashes")
        .map(decode_hashes)
        .unwrap_or(Ok(Vec::new()))?;

    let group = find_field(map, "group_idx")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;

    Ok(CacheEvent::Removed { hashes, group })
}
