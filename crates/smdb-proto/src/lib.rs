pub mod codec;
pub mod constants;
pub mod error;
pub mod frame;
pub mod messages;

pub use codec::{decode_message, encode_message};
pub use constants::{
    DEFAULT_METRICS_PORT, DEFAULT_PORT, MAX_FRAME_SIZE, PROTOCOL_VERSION, SERVER_VERSION,
};
pub use error::{ProtoError, Result};
pub use frame::{Frame, FrameCodec, FrameTag};
pub use messages::{
    AckMessage, AuthMessage, ChangeRecordMessage, CurrentMessage, DefineMachineMessage,
    ErrorMessage, HistoryMessage, NoticeMessage, ReadyMessage, RejectionMessage, ResultMessage,
    StartupMessage, SubscribeMessage, TransitionMessage, UnsubscribeMessage,
};

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    use super::*;
    use crate::messages::{AuthMessage, ReadyMessage, StartupMessage};

    fn roundtrip_frame(tag: FrameTag, body: bytes::Bytes) -> Frame {
        let original = Frame { tag, body };
        let mut buf = BytesMut::new();
        let mut codec = FrameCodec;
        codec.encode(original, &mut buf).expect("encode failed");

        codec
            .decode(&mut buf)
            .expect("decode error")
            .expect("expected a frame")
    }

    #[test]
    fn frame_codec_roundtrip_empty_body() {
        let frame = roundtrip_frame(FrameTag::Ping, bytes::Bytes::new());
        assert_eq!(frame.tag, FrameTag::Ping);
        assert!(frame.body.is_empty());
    }

    #[test]
    fn frame_codec_roundtrip_with_body() {
        let body = bytes::Bytes::from_static(b"hello world");
        let frame = roundtrip_frame(FrameTag::Auth, body.clone());
        assert_eq!(frame.tag, FrameTag::Auth);
        assert_eq!(frame.body, body);
    }

    #[test]
    fn frame_codec_partial_read() {
        let body = bytes::Bytes::from_static(b"partial");
        let full = Frame {
            tag: FrameTag::Ping,
            body,
        };
        let mut buf = BytesMut::new();
        let mut codec = FrameCodec;
        codec.encode(full, &mut buf).unwrap();

        // Feed only part of the buffer
        let mut partial = buf.split_to(3);
        assert!(codec.decode(&mut partial).unwrap().is_none());
    }

    #[test]
    fn frame_codec_rejects_oversized_frame() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[FrameTag::Auth as u8]);
        // Length field: 17 MB, exceeds MAX_FRAME_SIZE
        let too_big: u32 = 17 * 1024 * 1024;
        buf.extend_from_slice(&too_big.to_be_bytes());

        let mut codec = FrameCodec;
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, ProtoError::FrameTooLarge { .. }));
    }

    #[test]
    fn frame_codec_rejects_unknown_tag() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0xFF]); // unknown tag
        buf.extend_from_slice(&0u32.to_be_bytes()); // zero-length body

        let mut codec = FrameCodec;
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, ProtoError::InvalidFrameType(0xFF)));
    }

    #[test]
    fn message_encode_decode_startup() {
        let msg = StartupMessage {
            protocol_version: PROTOCOL_VERSION,
            capabilities: vec!["subscribe".into(), "history".into()],
        };
        let frame = encode_message(FrameTag::Startup, &msg).unwrap();
        assert_eq!(frame.tag, FrameTag::Startup);

        let decoded: StartupMessage = decode_message(&frame).unwrap();
        assert_eq!(decoded.protocol_version, PROTOCOL_VERSION);
        assert_eq!(decoded.capabilities, vec!["subscribe", "history"]);
    }

    #[test]
    fn message_encode_decode_auth() {
        let msg = AuthMessage {
            token: "secret-token".into(),
        };
        let frame = encode_message(FrameTag::Auth, &msg).unwrap();
        let decoded: AuthMessage = decode_message(&frame).unwrap();
        assert_eq!(decoded.token, "secret-token");
    }

    #[test]
    fn message_encode_decode_ready() {
        let msg = ReadyMessage {
            session_id: "sess-abc".into(),
            server_version: SERVER_VERSION.into(),
            protocol_version: PROTOCOL_VERSION,
            capabilities: vec!["subscribe".into()],
        };
        let frame = encode_message(FrameTag::Ready, &msg).unwrap();
        let decoded: ReadyMessage = decode_message(&frame).unwrap();
        assert_eq!(decoded.session_id, "sess-abc");
        assert_eq!(decoded.protocol_version, PROTOCOL_VERSION);
    }

    #[test]
    fn frame_codec_multiple_frames_in_buffer() {
        let mut buf = BytesMut::new();
        let mut codec = FrameCodec;

        let f1 = Frame {
            tag: FrameTag::Ping,
            body: bytes::Bytes::from_static(b"one"),
        };
        let f2 = Frame {
            tag: FrameTag::Pong,
            body: bytes::Bytes::from_static(b"two"),
        };
        codec.encode(f1, &mut buf).unwrap();
        codec.encode(f2, &mut buf).unwrap();

        let d1 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(d1.tag, FrameTag::Ping);
        assert_eq!(&d1.body[..], b"one");

        let d2 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(d2.tag, FrameTag::Pong);
        assert_eq!(&d2.body[..], b"two");

        assert!(codec.decode(&mut buf).unwrap().is_none());
    }
}
