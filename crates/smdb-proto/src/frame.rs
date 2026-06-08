use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::constants::MAX_FRAME_SIZE;
use crate::error::ProtoError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameTag {
    // Client → Server
    Startup = 0x01,
    Auth = 0x02,
    DefineMachine = 0x10,
    Transition = 0x11,
    Current = 0x12,
    History = 0x13,
    Subscribe = 0x20,
    Ack = 0x21,
    Unsubscribe = 0x22,
    Ping = 0x30,
    Terminate = 0x31,

    // Server → Client
    Ready = 0x80,
    AuthOk = 0x81,
    AuthError = 0x82,
    Result = 0x90,
    Rejection = 0x91,
    ChangeRecord = 0xA0,
    Notice = 0xB0,
    Error = 0xB1,
    Pong = 0xC0,
}

impl TryFrom<u8> for FrameTag {
    type Error = ProtoError;

    fn try_from(byte: u8) -> Result<Self, ProtoError> {
        match byte {
            0x01 => Ok(FrameTag::Startup),
            0x02 => Ok(FrameTag::Auth),
            0x10 => Ok(FrameTag::DefineMachine),
            0x11 => Ok(FrameTag::Transition),
            0x12 => Ok(FrameTag::Current),
            0x13 => Ok(FrameTag::History),
            0x20 => Ok(FrameTag::Subscribe),
            0x21 => Ok(FrameTag::Ack),
            0x22 => Ok(FrameTag::Unsubscribe),
            0x30 => Ok(FrameTag::Ping),
            0x31 => Ok(FrameTag::Terminate),
            0x80 => Ok(FrameTag::Ready),
            0x81 => Ok(FrameTag::AuthOk),
            0x82 => Ok(FrameTag::AuthError),
            0x90 => Ok(FrameTag::Result),
            0x91 => Ok(FrameTag::Rejection),
            0xA0 => Ok(FrameTag::ChangeRecord),
            0xB0 => Ok(FrameTag::Notice),
            0xB1 => Ok(FrameTag::Error),
            0xC0 => Ok(FrameTag::Pong),
            other => Err(ProtoError::InvalidFrameType(other)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub tag: FrameTag,
    pub body: Bytes,
}

// 1-byte tag + 4-byte BE length header
const HEADER_LEN: usize = 5;

#[derive(Default)]
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = ProtoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }

        let tag_byte = src[0];
        let body_len = u32::from_be_bytes([src[1], src[2], src[3], src[4]]);

        if body_len > MAX_FRAME_SIZE {
            return Err(ProtoError::FrameTooLarge {
                size: body_len,
                max: MAX_FRAME_SIZE,
            });
        }

        let total = HEADER_LEN + body_len as usize;
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }

        let tag = FrameTag::try_from(tag_byte)?;
        src.advance(HEADER_LEN);
        let body = src.split_to(body_len as usize).freeze();

        Ok(Some(Frame { tag, body }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = ProtoError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let body_len = frame.body.len() as u32;
        dst.reserve(HEADER_LEN + frame.body.len());
        dst.put_u8(frame.tag as u8);
        dst.put_u32(body_len);
        dst.put(frame.body);
        Ok(())
    }
}
