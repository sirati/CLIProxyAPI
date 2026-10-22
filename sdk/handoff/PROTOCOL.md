# Binary handoff protocol, version 1

Two CLIProxyAPI processes use this protocol to hand a running service from an
old binary to a new one without dropping a connection. The new process may be
any later (or earlier) build: both sides list what they understand, and
anything one side cannot take stays with the old process until it closes.

## Transport

A unix stream socket at the service's control path (systemd:
`$RUNTIME_DIRECTORY/handoff.sock`). The running service listens; the new
process connects.

Each frame is a 4-byte big-endian length followed by that many bytes of JSON.
File descriptors travel as `SCM_RIGHTS` attached to the frame's first byte (one
`sendmsg` per frame, at most 16 descriptors).

```json
{"v": 1, "type": "item", "kind": "conn.http1.v1", "id": "", "data": {...}, "error": ""}
```

`v` is the framing version (always 1). Payloads are versioned through `kind`:
a changed payload is a new kind (`.v2`), never a changed `.v1`. Unknown JSON
fields are ignored.

## Sequence

1. new → old `hello`, then old → new `hello`:
   `{"protocols": [1], "binary": "cliproxyapi 8.0.29", "pid": 1234, "capabilities": ["listener.tcp.v1", ...]}`.
   The old side refuses (`error`) unless the new side takes `listener.tcp.v1`.
2. new → old `takeover`. **The old process freezes**: it makes no further
   decision (credential selection, token refresh) and writes nothing (auth
   files, cooldown state). Requests that need a decision are re-run by the new
   process (see `conn.http1.v1`).
3. old → new state items, then `state-end`. Events may be interleaved.
4. The new process loads its auth files, applies the state, starts serving on
   the received listener and sends `ready`. If it does not, within two
   minutes, the old process takes its listener back, unfreezes and continues.
5. After `ready` the old process moves every connection as it reaches an idle
   point (`item`), forwards the side effects of the responses it still
   finishes (`event`), and sends `done` once it holds nothing transferable.
   It exits when its upstream relays (below) have ended.

## Kinds

| kind | direction | files | payload |
|---|---|---|---|
| `listener.tcp.v1` | item | listening TCP socket | none |
| `control.listener.v1` | item | this control socket's listener | none |
| `state.cooldown.v1` | item | – | list of cooldown records (as persisted with `save-cooldown-status`) |
| `state.affinity.v1` | item | – | list of session-affinity bindings `{auth_id, aliases, expires_at, binding_*}` |
| `state.usagequeue.v1` | item | – | usage payloads not yet consumed `{enqueued_at, payload}` |
| `conn.http1.v1` | item | client socket | `{carry}`: bytes read from the socket that the next owner must parse first (for a re-run: the whole request) |
| `conn.ws.responses.v1` | item | client socket, optional upstream stream | `/v1/responses` websocket session: upgrade request, handler state, tool caches, replayed messages, upstream state |
| `relay.plain.v1` | capability | – | the new side can take an upstream websocket as a plaintext stream (see below) |
| `event.result.v1` | event | – | an execution result to record (`MarkResult`) |
| `event.usage.v1` | event | – | a usage payload to enqueue |

## Connections

A plain HTTP/1.1 connection moves when it is between requests: no byte of a
next request has been handed to the HTTP server. A websocket moves between
messages: no part of a frame is outstanding. Neither carries hidden protocol
state at that point (websocket compression never uses context takeover), so the
socket descriptor and the carry bytes are everything the next owner needs.

An upstream TLS websocket cannot be moved, because its TLS state lives in the
old process. The old process instead passes one end of a socketpair and copies
plaintext between it and the TLS session ("relay") until either side closes.
The new process runs an ordinary websocket client over the stream after a
locally answered handshake. A process that received such a stream passes the
same descriptor on at the next handoff, so only the process owning the TLS
session keeps relaying.
