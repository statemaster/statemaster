# StateMaster Wire Protocol

**Version:** 1  
**Status:** v1 reference  
**Port:** 7632 (wire), 7633 (metrics/health)  
**Transport:** TCP + TLS (rustls)  
**Serialization:** MessagePack (bodies), big-endian (length prefix)

This document is the **driver contract** for StateMaster. Any client library, SDK, or tool that speaks to `smdbd` must implement exactly what is described here. The companion conformance test suite (`smdb-conformance`) is the normative check.

---

## Table of Contents

1. [Overview](#overview)
2. [Frame format](#frame-format)
3. [Frame type registry](#frame-type-registry)
4. [Handshake sequence](#handshake-sequence)
5. [Frame reference — client to server](#frame-reference--client-to-server)
6. [Frame reference — server to client](#frame-reference--server-to-client)
7. [Transition example end-to-end](#transition-example-end-to-end)
8. [Change-stream subscription](#change-stream-subscription)
9. [Rejection codes](#rejection-codes)
10. [Pipelining and correlation](#pipelining-and-correlation)
11. [Error handling and reconnection](#error-handling-and-reconnection)
12. [Protocol versioning](#protocol-versioning)

---

## Overview

The StateMaster wire protocol is a **custom binary protocol** over a long-lived, stateful TCP+TLS connection. It follows the Postgres connection model: one persistent connection per client (typically pooled), a handshake that establishes identity and capabilities, then bidirectional typed message frames for the life of the session.

Design properties:

- **Binary and compact.** Frames use a 6-byte fixed header followed by a MessagePack body. No JSON, no HTTP, no gRPC, no IDL/codegen toolchain.
- **Stateful session.** After the handshake the connection holds identity, negotiated capabilities, and subscription cursors. There is no per-request authentication overhead.
- **Pipelined.** Clients can send multiple frames before reading replies. Every command frame carries a `request_id`; every reply echoes it. Command replies and async push events interleave cleanly on the same socket.
- **Async push.** `ChangeRecord` frames arrive unsolicited as transitions commit. They carry a `subscription_id` to distinguish them from command replies.

---

## Frame format

Every message, in both directions, is a **frame**:

```
┌─────────────┬───────────────────────────┬──────────────────────────────────┐
│  tag (1B)   │  length (4B, big-endian)  │  body (length bytes, MessagePack) │
└─────────────┴───────────────────────────┴──────────────────────────────────┘
```

| Field    | Size     | Encoding          | Description                                    |
|----------|----------|-------------------|------------------------------------------------|
| `tag`    | 1 byte   | unsigned integer  | Frame type; see registry below                 |
| `length` | 4 bytes  | big-endian uint32 | Byte length of the body that follows           |
| `body`   | variable | MessagePack map   | Frame-specific fields; keys are string names   |

**Frame length cap:** the server rejects any frame whose declared `length` exceeds 16 MiB (16,777,216 bytes). The server sends `Error` with code `frame_too_large` and closes the connection.

**Body encoding:** MessagePack maps with string keys. This makes the format self-describing and easy to extend: unknown keys are ignored, preserving forward compatibility.

---

## Frame type registry

### Client to server

| Tag    | Hex    | Name            | Description                                      |
|--------|--------|-----------------|--------------------------------------------------|
| `0x01` | 1      | `Startup`       | Open protocol negotiation                        |
| `0x02` | 2      | `Auth`          | Authenticate the session with a bearer token     |
| `0x10` | 16     | `DefineMachine` | Register or update a machine definition          |
| `0x11` | 17     | `Transition`    | Fire an event against an entity                  |
| `0x12` | 18     | `Current`       | Read an entity's current state                   |
| `0x13` | 19     | `History`       | Read an entity's transition log                  |
| `0x20` | 32     | `Subscribe`     | Begin a change-stream subscription               |
| `0x21` | 33     | `Ack`           | Advance a subscription cursor                    |
| `0x22` | 34     | `Unsubscribe`   | Stop a subscription                              |
| `0x30` | 48     | `Ping`          | Keepalive probe                                  |
| `0x31` | 49     | `Terminate`     | Graceful session close                           |

### Server to client

| Tag    | Hex    | Name           | Description                                        |
|--------|--------|----------------|----------------------------------------------------|
| `0x80` | 128    | `Ready`        | Handshake complete; session open                   |
| `0x81` | 129    | `AuthOk`       | Authentication accepted                            |
| `0x82` | 130    | `AuthError`    | Authentication rejected                            |
| `0x90` | 144    | `Result`       | Successful reply to a command                      |
| `0x91` | 145    | `Rejection`    | Typed rejection of a command                       |
| `0xA0` | 160    | `ChangeRecord` | Async change-stream push (unsolicited)             |
| `0xB0` | 176    | `Notice`       | Non-fatal informational message from the server    |
| `0xB1` | 177    | `Error`        | Fatal error; connection will be closed             |
| `0xC0` | 192    | `Pong`         | Keepalive reply                                    |

Tag ranges are reserved: `0x03–0x0F` (future session-level), `0x14–0x1F` (future verbs), `0x23–0x2F` (future stream controls), `0x32–0x7F` (future client), `0x83–0x8F` (future server handshake), `0x92–0x9F` (future replies), `0xA1–0xBF` (future push), `0xC1–0xFF` (future server).

---

## Handshake sequence

A new connection goes through exactly this sequence before any application frames are accepted:

```
Client                                    Server
  │                                          │
  │── TCP connect :7632 ────────────────────>│
  │<──────────────────── TLS handshake ──────│
  │                                          │
  │── Startup (0x01) ───────────────────────>│   protocol_version, capabilities
  │── Auth    (0x02) ───────────────────────>│   token
  │                                          │
  │<── AuthOk  (0x81) ──────────────────────│   (if token accepted)
  │<── Ready   (0x80) ──────────────────────│   session_id, server_version, capabilities
  │                                          │
  │  ~~~ session open — commands flow ~~~   │
  │                                          │
  │── Terminate (0x31) ─────────────────────>│   (graceful close)
```

**Step-by-step:**

1. **TCP connect** to `:7632`.
2. **TLS handshake** (`rustls`). The server presents its certificate; the client should validate against a trusted CA or pinned certificate.
3. **Client sends `Startup`.** Must be the first frame on the wire. Contains the client's desired protocol version and its supported capabilities. The server uses this to negotiate down to the highest mutually supported version.
4. **Client sends `Auth`** immediately after `Startup`, without waiting for a reply. This saves a round trip. The server processes `Startup` then `Auth` in order.
5. **Server sends `AuthOk`** if the token is valid. If invalid, the server sends `AuthError` and closes the connection.
6. **Server sends `Ready`** after `AuthOk`. This frame carries the session ID, the server's version string, and the negotiated capabilities. The session is now open.

If `Auth` is not received within the connection timeout (default 5 s) after `Startup`, the server closes the connection with an `Error`.

---

## Frame reference — client to server

### `Startup` (0x01)

Sent as the first frame on every new connection.

| Field              | Type            | Required | Description                                   |
|--------------------|-----------------|----------|-----------------------------------------------|
| `protocol_version` | uint            | yes      | Client's desired protocol version. Currently `1`. |
| `capabilities`     | array of string | no       | Feature flags the client supports (reserved for future use). |
| `driver_name`      | string          | no       | Human-readable driver name for server-side logging (e.g. `"smdb-sdk-rust/0.1.0"`). |

### `Auth` (0x02)

Authenticates the session. Sent immediately after `Startup`.

| Field   | Type   | Required | Description                 |
|---------|--------|----------|-----------------------------|
| `token` | string | yes      | Bearer token / API key.     |

### `DefineMachine` (0x10)

Registers a new machine definition or a new version of an existing one. Idempotent for the same `(name, version)` pair with identical content.

| Field        | Type   | Required | Description                                           |
|--------------|--------|----------|-------------------------------------------------------|
| `request_id` | uint   | yes      | Client-chosen correlation ID; echoed in the reply.    |
| `name`       | string | yes      | Machine name (e.g. `"fulfillment"`).                  |
| `version`    | uint   | yes      | Definition version number; starts at 1.               |
| `definition` | map    | yes      | Machine definition body (states, transitions, guards, effects). Schema defined in `smdb-core`. |

Reply: `Result` with `machine_ref` payload, or `Rejection`.

### `Transition` (0x11)

Fires an event against an entity and requests a state transition.

| Field              | Type   | Required | Description                                                              |
|--------------------|--------|----------|--------------------------------------------------------------------------|
| `request_id`       | uint   | yes      | Correlation ID.                                                          |
| `entity_id`        | string | yes      | Opaque entity identifier (e.g. `"order_8412"`).                          |
| `machine`          | string | yes      | Machine name.                                                            |
| `event`            | string | yes      | The event to fire (e.g. `"ship"`). Never a target state.                 |
| `expected_version` | uint   | no       | If present, the transition is rejected with `version_conflict` if the entity's current version differs. Enables optimistic concurrency. |
| `idempotency_key`  | string | no       | Client-generated UUID or opaque token. A second `Transition` with the same key returns the original `Result` rather than re-executing. |
| `ctx`              | map    | no       | Arbitrary MessagePack map of caller-supplied context (e.g. `{"carrier": "ups"}`). Stored on the transition record. |
| `actor`            | string | no       | Identity of the actor performing this transition (e.g. `"svc:fulfillment-worker"`). Stored on the record and surfaced on change records. Defaults to the authenticated session identity. |

Reply: `Result` with transition payload, or `Rejection`.

### `Current` (0x12)

Reads an entity's current state from the projection (does not take locks).

| Field        | Type   | Required | Description         |
|--------------|--------|----------|---------------------|
| `request_id` | uint   | yes      | Correlation ID.     |
| `entity_id`  | string | yes      | Entity identifier.  |
| `machine`    | string | yes      | Machine name.       |

Reply: `Result` with `{entity_id, machine, state, version, updated_at}`.

### `History` (0x13)

Reads an entity's transition log.

| Field        | Type   | Required | Description                                                       |
|--------------|--------|----------|-------------------------------------------------------------------|
| `request_id` | uint   | yes      | Correlation ID.                                                   |
| `entity_id`  | string | yes      | Entity identifier.                                                |
| `machine`    | string | yes      | Machine name.                                                     |
| `after_seq`  | uint   | no       | Return only transitions with `sequence` strictly greater than this. Defaults to `0` (full history). |
| `limit`      | uint   | no       | Maximum number of records to return. Defaults to 1000.            |

Reply: `Result` with `{records: [...]}` where each record matches the transition log schema.

### `Subscribe` (0x20)

Begins a change-stream subscription. Once accepted, the server will push `ChangeRecord` frames asynchronously as transitions commit. Multiple subscriptions may exist on one connection, each with its own `subscription_id`.

| Field             | Type   | Required | Description                                                                   |
|-------------------|--------|----------|-------------------------------------------------------------------------------|
| `request_id`      | uint   | yes      | Correlation ID for the subscribe reply.                                       |
| `subscription_id` | string | yes      | Client-chosen opaque string; echoed on every `ChangeRecord` for this stream.  |
| `after_sequence`  | uint   | yes      | Receive records with `sequence` strictly greater than this. Use `0` to start from the beginning (full backfill). |
| `filter_machine`  | string | no       | If set, only deliver records for this machine.                                |
| `filter_entity`   | string | no       | If set, only deliver records for this entity ID.                              |

Reply: `Result` with `{subscription_id, cursor}` confirming the subscription, followed by a stream of `ChangeRecord` frames.

### `Ack` (0x21)

Advances the subscription cursor. Records at or below `up_to_sequence` will not be redelivered.

| Field             | Type   | Required | Description                          |
|-------------------|--------|----------|--------------------------------------|
| `subscription_id` | string | yes      | Which subscription to advance.       |
| `up_to_sequence`  | uint   | yes      | The highest sequence the client has processed. |

No reply (fire-and-forget). The server updates the cursor in memory.

### `Unsubscribe` (0x22)

Stops a subscription. No more `ChangeRecord` frames will be pushed for this `subscription_id`.

| Field             | Type   | Required | Description               |
|-------------------|--------|----------|---------------------------|
| `request_id`      | uint   | yes      | Correlation ID.           |
| `subscription_id` | string | yes      | Subscription to cancel.   |

Reply: `Result` with `{subscription_id}`.

### `Ping` (0x30)

Keepalive probe. The body may be empty or carry an opaque `payload` bytes field. The server replies with `Pong` echoing the payload.

| Field     | Type  | Required | Description               |
|-----------|-------|----------|---------------------------|
| `payload` | bytes | no       | Echoed in the `Pong`.     |

### `Terminate` (0x31)

Requests a graceful session close. The server drains any in-flight replies, then closes the connection. Body may be empty.

---

## Frame reference — server to client

### `Ready` (0x80)

Sent after `AuthOk` to signal the session is open.

| Field              | Type            | Description                                         |
|--------------------|-----------------|-----------------------------------------------------|
| `session_id`       | string          | Server-assigned opaque session identifier.          |
| `server_version`   | string          | Server binary version string (e.g. `"0.1.0"`).      |
| `protocol_version` | uint            | Negotiated protocol version (will be `<= Startup`'s requested version). |
| `capabilities`     | array of string | Server-supported capabilities, intersected with client's. |

### `AuthOk` (0x81)

Authentication accepted. No additional fields required; body may be empty or carry a `message` string.

### `AuthError` (0x82)

Authentication rejected. The connection will be closed after this frame.

| Field     | Type   | Description                      |
|-----------|--------|----------------------------------|
| `message` | string | Human-readable rejection reason. |

### `Result` (0x90)

Successful reply to a command.

| Field        | Type   | Description                                    |
|--------------|--------|------------------------------------------------|
| `request_id` | uint   | Echoes the `request_id` from the command.      |
| `payload`    | map    | Command-specific result fields (see per-verb descriptions above). |

### `Rejection` (0x91)

Typed, non-fatal rejection of a command. The session remains open.

| Field           | Type   | Description                                                         |
|-----------------|--------|---------------------------------------------------------------------|
| `request_id`    | uint   | Echoes the `request_id` from the command.                           |
| `code`          | string | Machine-readable rejection code (see [Rejection codes](#rejection-codes)). |
| `message`       | string | Human-readable explanation.                                         |
| `current_state` | string | Present on `Transition` rejections: the entity's actual current state at rejection time. |
| `version`       | uint   | Present on `Transition` rejections: the entity's actual version at rejection time. |

### `ChangeRecord` (0xA0)

Async push frame. Sent unsolicited whenever a transition commits and the subscription filter matches. No `request_id` — identified by `subscription_id`.

| Field             | Type            | Description                                                            |
|-------------------|-----------------|------------------------------------------------------------------------|
| `subscription_id` | string          | Which subscription this record belongs to.                             |
| `sequence`        | uint            | Monotonic, gap-free global sequence number. Use this as your cursor.   |
| `transition_id`   | string          | Unique transition identifier (use for consumer-side deduplication).    |
| `entity_id`       | string          | Entity that transitioned.                                              |
| `machine`         | string          | Machine name.                                                          |
| `from`            | string          | State before the transition.                                           |
| `to`              | string          | State after the transition.                                            |
| `event`           | string          | The event that was fired.                                              |
| `actor`           | string          | Identity of the actor.                                                 |
| `version`         | uint            | Entity's post-transition version.                                      |
| `ts`              | string          | RFC 3339 timestamp of the transition.                                  |
| `ctx`             | map             | Caller-supplied context from the `Transition` frame.                   |
| `effects`         | array of map    | Effects emitted by this transition. Each has `type` and `payload`.     |

### `Notice` (0xB0)

Non-fatal server message. The session remains open.

| Field     | Type   | Description             |
|-----------|--------|-------------------------|
| `message` | string | Informational text.     |
| `code`    | string | Optional notice code.   |

### `Error` (0xB1)

Fatal error. The server will close the connection after sending this frame.

| Field     | Type   | Description                     |
|-----------|--------|---------------------------------|
| `code`    | string | Machine-readable error code.    |
| `message` | string | Human-readable explanation.     |

### `Pong` (0xC0)

Keepalive reply to a `Ping`.

| Field     | Type  | Description                             |
|-----------|-------|-----------------------------------------|
| `payload` | bytes | Echoed from the `Ping` frame, if present. |

---

## Transition example end-to-end

This is the complete frame exchange for a `ship` transition on `order_8412`.

**Client sends `Transition` (tag `0x11`):**

```
Header: [0x11][0x00 0x00 0x00 6A]   (tag=0x11, body length=106 bytes)
Body (MessagePack map, shown as JSON for readability):
{
  "request_id":        42,
  "entity_id":         "order_8412",
  "machine":           "fulfillment",
  "event":             "ship",
  "expected_version":  3,
  "idempotency_key":   "3f8c1a9e-bb4d-4c2a-a3e1-c1d5f7890abc",
  "ctx":               { "carrier": "ups", "tracking": "1Z999AA10123456784" },
  "actor":             "svc:fulfillment-worker"
}
```

**Server replies with `Result` (tag `0x90`) on success:**

```
Header: [0x90][0x00 0x00 0x00 ...]
Body:
{
  "request_id":     42,
  "payload": {
    "entity_id":      "order_8412",
    "machine":        "fulfillment",
    "from":           "packed",
    "state":          "shipped",
    "version":        4,
    "transition_id":  "txn_01J9ZQ8M3K",
    "sequence":       100428,
    "ts":             "2026-06-01T15:04:05Z"
  }
}
```

**Or `Rejection` (tag `0x91`) on failure:**

```
Header: [0x91][0x00 0x00 0x00 ...]
Body:
{
  "request_id":     42,
  "code":           "illegal_transition",
  "message":        "no transition for event 'ship' from state 'delivered'",
  "current_state":  "delivered",
  "version":        7
}
```

Shortly after a successful transition, the change-stream dispatcher publishes a `ChangeRecord` (tag `0xA0`) to any matching subscriber on any connection:

```
Header: [0xA0][0x00 0x00 0x00 ...]
Body:
{
  "subscription_id": "sub_orders_1",
  "sequence":        100428,
  "transition_id":   "txn_01J9ZQ8M3K",
  "entity_id":       "order_8412",
  "machine":         "fulfillment",
  "from":            "packed",
  "to":              "shipped",
  "event":           "ship",
  "actor":           "svc:fulfillment-worker",
  "version":         4,
  "ts":              "2026-06-01T15:04:05Z",
  "ctx":             { "carrier": "ups", "tracking": "1Z999AA10123456784" },
  "effects": [
    { "type": "notify", "payload": { "template": "shipped", "to": "customer" } }
  ]
}
```

---

## Change-stream subscription

### Subscribing

Send a `Subscribe` frame with an `after_sequence` cursor. The server immediately begins delivering matching `ChangeRecord` frames, starting with any records already committed above the cursor (backfill), then streams new records as they commit.

```
Client                             Server
  │── Subscribe (sub_id="s1",        │
  │    after_sequence=100000) ──────>│
  │<── Result (sub confirmed) ───────│
  │                                  │
  │<── ChangeRecord (seq=100001) ────│  (backfill)
  │<── ChangeRecord (seq=100002) ────│  (backfill)
  │       ...                        │
  │                                  │
  │     [new transition commits]     │
  │<── ChangeRecord (seq=100428) ────│  (live push)
  │                                  │
  │── Ack (up_to=100428) ───────────>│  (advance cursor)
```

### Cursor semantics

- The cursor is the `sequence` of the last record the subscriber has successfully processed.
- Send `Ack(up_to=N)` after processing record `N`. Unacked records are redelivered on reconnect if the subscriber reconnects with the same `after_sequence`.
- `Ack` is fire-and-forget; it does not generate a `Result`. It is safe to batch: ack every N records rather than every record.

### Ordering guarantees

- **Per-entity total order** is guaranteed: transitions for a given `entity_id` always arrive in the order they committed.
- **Global total order** via `sequence` is guaranteed on a single-node deployment (v1). This weakens to per-entity order once sharding lands in v3.

### Delivery guarantees

- **At-least-once.** A crash between commit and dispatch causes re-delivery, never loss. Consumers must deduplicate on `transition_id` or `sequence`.
- **No gaps.** The `sequence` counter is monotonic and gap-free; a subscriber that receives sequences 100, 101, 103 without receiving 102 indicates a bug.

> **v1 implementation note.** Both backfill and live tail are delivered by a single path: a background **dispatcher** reads the committed transition log and fans change records out to subscribers, advancing a per-subscriber cursor. Because the cursor only advances once a record has been handed to the connection, and because the log (not an in-memory queue) is the source of truth, delivery is at-least-once and replayable — a subscriber that reconnects with its last `after_sequence` re-reads anything it missed. Consumers must dedupe on `sequence`/`transition_id`.

### Multiple subscriptions

A single connection supports multiple independent subscriptions. Each has its own `subscription_id`, its own cursor, and its own filter. `ChangeRecord` frames from different subscriptions interleave freely; the `subscription_id` field distinguishes them.

---

## Rejection codes

All rejection codes are lowercase snake_case strings. They appear in the `code` field of `Rejection` frames.

### Transition rejections

| Code                 | Meaning                                                                           |
|----------------------|-----------------------------------------------------------------------------------|
| `illegal_transition` | No entry in the machine's transition table for `(current_state, event)`.         |
| `guard_failed`       | A guard predicate for this transition evaluated to false. The `message` names the guard. |
| `version_conflict`   | `expected_version` was supplied and does not match the entity's current version.  |
| `unknown_machine`    | No machine with the given name is registered.                                     |
| `unknown_entity`     | The entity has no state record for this machine (has never been transitioned).    |
| `duplicate_idempotency_key` | The `idempotency_key` matches a previous transition on a *different* entity+event combination (same key, different intent). |

### Session / connection rejections

| Code              | Meaning                                                                   |
|-------------------|---------------------------------------------------------------------------|
| `unauthenticated` | A command was sent before a successful `Auth` exchange.                   |
| `unauthorized`    | The authenticated session lacks permission for this operation.            |
| `bad_request`     | The frame body is malformed, missing required fields, or contains invalid values. |
| `frame_too_large` | The frame's declared `length` exceeds the server's maximum (16 MiB). Fatal — connection closed. |
| `protocol_error`  | Unexpected frame type for the current session state (e.g. `Startup` after session is open). Fatal — connection closed. |

### DefineMachine rejections

| Code                   | Meaning                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `invalid_definition`   | The machine definition body fails structural validation.                |
| `version_already_exists` | A machine with this `(name, version)` exists with different content. |

---

## Pipelining and correlation

Clients may send multiple command frames without waiting for replies. The `request_id` field correlates each reply to its command. The server processes commands in the order received per connection and delivers replies in that same order.

`ChangeRecord` frames are pushed asynchronously and may interleave with command replies. They are identified by `subscription_id`, not `request_id`. Clients must handle frames in any order and dispatch on the combination of tag and correlation field.

Recommended client implementation pattern:

```
read loop:
  frame = read_frame(conn)
  if frame.tag == 0xA0:
    dispatch to subscription handler by subscription_id
  else:
    dispatch to pending-command handler by request_id
```

---

## Error handling and reconnection

### Connection-level errors

`Error` (0xB1) frames are fatal. The server closes the connection after sending one. The client should treat any `Error` as requiring a full reconnect and re-authentication.

### Transient errors

`Rejection` (0x91) frames are non-fatal. The session remains open. The client application should handle the typed `code` (retry on `version_conflict` with a refreshed version; surface `illegal_transition` as a domain error; etc.).

### Reconnection

On connection loss or `Error`, the client should:

1. Re-establish the TCP+TLS connection.
2. Replay the full handshake (`Startup`, `Auth`).
3. Re-issue `Subscribe` frames for any active subscriptions, providing the last successfully acked `sequence` as `after_sequence` to resume without missing records.
4. Re-issue any in-flight commands that were not acknowledged (use `idempotency_key` on `Transition` frames to make retries safe).

---

## Protocol versioning

The protocol version is negotiated in the `Startup` / `Ready` exchange. The client sends its desired version; the server responds with the highest mutually supported version in `Ready.protocol_version`.

**Version 1** (this document) is the initial version. All fields documented here with "Required: yes" must be present. Unknown fields in the body are ignored — this is the forward-compatibility rule.

When a new version introduces breaking changes:
- A new tag value or a new required field in an existing frame constitutes a breaking change and requires a version increment.
- Adding an optional field to an existing frame is non-breaking.
- The server will never send frames with tags from the "reserved" ranges documented in the type registry until a protocol version introducing them is negotiated.
