use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::engine::Direction;
use crate::error::Error;
use crate::region::RegionId;

pub(crate) const MAXFRAME: usize = 64 << 20;
pub(crate) const HANDSHAKELIMIT: usize = 4096;
pub(crate) const QUEUE: usize = 64;
const MAGIC: [u8; 4] = *b"KVTE";
const VERSION: u8 = 2;
pub(crate) const PREFIX: usize = 5;
const REQUEST: usize = 41;
const RESPONSE: usize = 9;

pub(crate) const OVERHEAD: usize = 64;

pub(crate) struct Request {
    pub id: u64,
    pub direction: Direction,
    pub region: RegionId,
    pub offset: u64,
    pub len: usize,
    pub data: Vec<u8>,
}

impl Request {
    pub(crate) fn encode(self) -> Vec<u8> {
        let mut frame = Vec::with_capacity(REQUEST + self.data.len());
        frame.extend_from_slice(&self.id.to_be_bytes());
        frame.push(match self.direction {
            Direction::Read => 0,
            Direction::Write => 1,
        });
        frame.extend_from_slice(&self.region.0.to_be_bytes());
        frame.extend_from_slice(&self.offset.to_be_bytes());
        frame.extend_from_slice(&(self.len as u64).to_be_bytes());
        frame.extend_from_slice(&self.data);
        frame
    }

    pub(crate) fn decode(frame: Vec<u8>) -> Result<Self, Error> {
        if frame.len() < REQUEST {
            return Err(Error::Protocol("short request".into()));
        }
        let id = u64::from_be_bytes(frame[0..8].try_into().expect("fixed width"));
        let direction = match frame[8] {
            0 => Direction::Read,
            1 => Direction::Write,
            _ => return Err(Error::Protocol("unknown direction".into())),
        };
        let region = RegionId(u128::from_be_bytes(
            frame[9..25].try_into().expect("fixed width"),
        ));
        let offset = u64::from_be_bytes(frame[25..33].try_into().expect("fixed width"));
        let len = usize::try_from(u64::from_be_bytes(
            frame[33..41].try_into().expect("fixed width"),
        ))
        .map_err(|_| Error::Size)?;
        let data = frame[REQUEST..].to_vec();
        match direction {
            Direction::Read if !data.is_empty() => {
                return Err(Error::Protocol("read request carries data".into()));
            }
            Direction::Write if data.len() != len => {
                return Err(Error::Protocol("write length mismatch".into()));
            }
            _ => {}
        }
        Ok(Self {
            id,
            direction,
            region,
            offset,
            len,
            data,
        })
    }
}

pub(crate) struct Response<'a> {
    pub id: u64,
    pub status: u8,
    pub body: &'a [u8],
}

impl<'a> Response<'a> {
    pub(crate) fn encode(id: u64, status: u8, body: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(RESPONSE + body.len());
        frame.extend_from_slice(&id.to_be_bytes());
        frame.push(status);
        frame.extend_from_slice(body);
        frame
    }

    pub(crate) fn decode(frame: &'a [u8]) -> Result<Self, Error> {
        if frame.len() < RESPONSE {
            return Err(Error::Protocol("short response".into()));
        }
        Ok(Self {
            id: u64::from_be_bytes(frame[0..8].try_into().expect("fixed width")),
            status: frame[8],
            body: &frame[RESPONSE..],
        })
    }
}

pub(crate) async fn recv<R>(reader: &mut R, max: usize) -> Result<Vec<u8>, Error>
where
    R: AsyncRead + Unpin,
{
    let mut head = [0; 4];
    reader.read_exact(&mut head).await?;
    let len = u32::from_be_bytes(head) as usize;
    if len > max {
        return Err(Error::Size);
    }
    let mut frame = vec![0; len];
    reader.read_exact(&mut frame).await?;
    Ok(frame)
}

pub(crate) async fn send<W>(writer: &mut W, frame: &[u8]) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let len = u32::try_from(frame.len()).map_err(|_| Error::Size)?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(frame).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) async fn pump<W>(mut writer: W, mut frames: mpsc::Receiver<Vec<u8>>) -> bool
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = frames.recv().await {
        if send(&mut writer, &frame).await.is_err() {
            return false;
        }
    }
    true
}

pub(crate) fn handshake(token: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(PREFIX + token.len());
    frame.extend_from_slice(&MAGIC);
    frame.push(VERSION);
    frame.extend_from_slice(token);
    frame
}

pub(crate) fn authenticated(frame: &[u8], token: &[u8]) -> bool {
    use subtle::ConstantTimeEq;

    frame.len() == PREFIX + token.len()
        && frame[..4] == MAGIC
        && frame[4] == VERSION
        && bool::from(frame[5..].ct_eq(token))
}
