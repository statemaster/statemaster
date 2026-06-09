//! End-to-end tests that drive a real `Server` over a TCP socket using the
//! wire protocol. These exercise the engine from inside the async runtime —
//! the exact condition under which the entity lock previously panicked — so a
//! regression in the transition path fails here rather than silently in prod.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_util::codec::Framed;

use smdb_core::prelude::{MachineBuilder, MachineDefinition};
use smdb_engine::{Dispatcher, Engine};
use smdb_proto::codec::{decode_message, encode_message};
use smdb_proto::frame::{Frame, FrameCodec, FrameTag};
use smdb_proto::messages::{
    AuthMessage, ChangeRecordMessage, ResultMessage, StartupMessage, SubscribeMessage,
    TransitionMessage,
};
use smdb_proto::{MAX_FRAME_SIZE, PROTOCOL_VERSION};
use smdb_storage::RedbEngine;
use smdb_wire::{Server, ServerConfig};

const TOKEN: &str = "test-token";

fn order_machine() -> MachineDefinition {
    MachineBuilder::new()
        .name("fulfillment")
        .version(1)
        .states(["pending", "paid", "packed", "shipped"])
        .initial_state("pending")
        .transition("pay", ["pending"], "paid")
        .transition("pack", ["paid"], "packed")
        .build()
        .unwrap()
}

/// Bind an ephemeral port, start the server and a dispatcher, and return the
/// address. The returned shutdown sender must be kept alive for the duration of
/// the test (dropping it would stop the dispatcher).
async fn start_server() -> (SocketAddr, watch::Sender<bool>) {
    let storage = Arc::new(RedbEngine::in_memory().unwrap());
    let engine = Arc::new(Engine::new(storage));
    engine.define_machine(order_machine()).unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    {
        let eng = Arc::clone(&engine);
        let sd = shutdown_rx.clone();
        tokio::spawn(async move {
            Dispatcher::new(eng, Duration::from_millis(10), sd)
                .run()
                .await;
        });
    }

    let config = ServerConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        tls_cert_path: None,
        tls_key_path: None,
        auth_tokens: vec![TOKEN.to_string()],
        max_connections: 16,
        max_frame_size: MAX_FRAME_SIZE,
    };
    let server = Server::new(config, engine).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    (addr, shutdown_tx)
}

async fn next_frame(framed: &mut Framed<TcpStream, FrameCodec>) -> Frame {
    framed
        .next()
        .await
        .expect("connection closed unexpectedly")
        .expect("frame decode error")
}

/// Connect, run the Startup + Auth handshake, and return the framed stream
/// positioned in the open-session state.
async fn connect_and_handshake(addr: SocketAddr) -> Framed<TcpStream, FrameCodec> {
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut framed = Framed::new(stream, FrameCodec);

    framed
        .send(
            encode_message(
                FrameTag::Startup,
                &StartupMessage {
                    protocol_version: PROTOCOL_VERSION,
                    capabilities: vec![],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    framed
        .send(
            encode_message(
                FrameTag::Auth,
                &AuthMessage {
                    token: TOKEN.to_string(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(next_frame(&mut framed).await.tag, FrameTag::AuthOk);
    assert_eq!(next_frame(&mut framed).await.tag, FrameTag::Ready);
    framed
}

fn transition_frame(request_id: u64, entity: &str, event: &str, idem: Option<&str>) -> Frame {
    encode_message(
        FrameTag::Transition,
        &TransitionMessage {
            request_id,
            entity_id: entity.to_string(),
            machine: "fulfillment".to_string(),
            event: event.to_string(),
            expected_version: None,
            idempotency_key: idem.map(|s| s.to_string()),
            ctx: serde_json::json!({}),
            actor: "user:test".to_string(),
        },
    )
    .unwrap()
}

#[tokio::test]
async fn transition_over_the_wire_succeeds() {
    let (addr, _shutdown) = start_server().await;
    let mut framed = connect_and_handshake(addr).await;

    framed
        .send(transition_frame(1, "order_1", "pay", None))
        .await
        .unwrap();

    let reply = next_frame(&mut framed).await;
    assert_eq!(
        reply.tag,
        FrameTag::Result,
        "expected Result, got {:?}",
        reply.tag
    );
    let result: ResultMessage = decode_message(&reply).unwrap();
    assert_eq!(result.request_id, 1);
    assert_eq!(result.payload["state"], "paid");
    assert_eq!(result.payload["from"], "pending");
    assert_eq!(result.payload["version"], 1);
}

#[tokio::test]
async fn illegal_transition_is_rejected_not_fatal() {
    let (addr, _shutdown) = start_server().await;
    let mut framed = connect_and_handshake(addr).await;

    // "pack" requires "paid"; entity starts at "pending".
    framed
        .send(transition_frame(7, "order_2", "pack", None))
        .await
        .unwrap();

    let reply = next_frame(&mut framed).await;
    assert_eq!(reply.tag, FrameTag::Rejection);
}

#[tokio::test]
async fn idempotent_retry_replays_same_result_with_real_version() {
    let (addr, _shutdown) = start_server().await;
    let mut framed = connect_and_handshake(addr).await;

    framed
        .send(transition_frame(1, "order_3", "pay", Some("key-abc")))
        .await
        .unwrap();
    let first: ResultMessage = decode_message(&next_frame(&mut framed).await).unwrap();

    // Same idempotency key — must replay the original result rather than erroring
    // or re-running (the entity is now "paid", where "pay" would be illegal).
    framed
        .send(transition_frame(2, "order_3", "pay", Some("key-abc")))
        .await
        .unwrap();
    let replay = next_frame(&mut framed).await;
    assert_eq!(
        replay.tag,
        FrameTag::Result,
        "replay should not be a rejection"
    );
    let second: ResultMessage = decode_message(&replay).unwrap();

    assert_eq!(second.payload["sequence"], first.payload["sequence"]);
    assert_eq!(
        second.payload["transition_id"],
        first.payload["transition_id"]
    );
    // Regression guard: replay previously returned version 0.
    assert_eq!(second.payload["version"], 1);
}

#[tokio::test]
async fn subscriber_receives_pushed_change_record() {
    let (addr, _shutdown) = start_server().await;

    // Subscriber connection: subscribe from the beginning of the stream.
    let mut sub = connect_and_handshake(addr).await;
    sub.send(
        encode_message(
            FrameTag::Subscribe,
            &SubscribeMessage {
                request_id: 100,
                subscription_id: "s1".to_string(),
                machine_filter: Some("fulfillment".to_string()),
                after_sequence: 0,
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    // First reply is the Subscribe ack.
    assert_eq!(next_frame(&mut sub).await.tag, FrameTag::Result);

    // A second connection drives a transition that should be pushed to the subscriber.
    let mut writer = connect_and_handshake(addr).await;
    writer
        .send(transition_frame(1, "order_9", "pay", None))
        .await
        .unwrap();
    assert_eq!(next_frame(&mut writer).await.tag, FrameTag::Result);

    // The dispatcher fans the committed transition out to the subscriber.
    let pushed = next_frame(&mut sub).await;
    assert_eq!(pushed.tag, FrameTag::ChangeRecord);
    let msg: ChangeRecordMessage = decode_message(&pushed).unwrap();
    assert_eq!(msg.subscription_id, "s1");
    assert_eq!(msg.record.entity_id, "order_9");
    assert_eq!(msg.record.event, "pay");
    assert_eq!(msg.record.to_state, "paid");
    assert_eq!(msg.record.version, 1);
    assert_eq!(msg.record.sequence, 1);
}
