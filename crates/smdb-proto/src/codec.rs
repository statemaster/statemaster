use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};

use crate::error::{ProtoError, Result};
use crate::frame::{Frame, FrameTag};

pub fn encode_message<T: Serialize>(tag: FrameTag, msg: &T) -> Result<Frame> {
    let body = rmp_serde::to_vec_named(msg)
        .map_err(|e| ProtoError::SerializationError(e.to_string()))?;
    Ok(Frame {
        tag,
        body: Bytes::from(body),
    })
}

pub fn decode_message<T: DeserializeOwned>(frame: &Frame) -> Result<T> {
    rmp_serde::from_slice(&frame.body)
        .map_err(|e| ProtoError::DeserializationError(e.to_string()))
}
