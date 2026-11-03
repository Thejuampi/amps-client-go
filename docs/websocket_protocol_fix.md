# AMPS WebSocket protocol correction

## Defect and source of the false positive

AMPS 5.3.5.60 was reported to close the connection before Go completed logon.
The adapter sent a length-prefixed TCP AMPS command in one WebSocket message.
fakeamps accepted that same format, so the tests did not exercise the real
WebSocket protocol.

The [server transport documentation](https://crankuptheamps.com/docs/amps-user-guide/transports/client-connections)
distinguishes WebSocket framing from the TCP protocol. The official
[JavaScript client](https://crankuptheamps.com/client-apis/javascript) is the
reference for this transport; the native C++ client is the reference for the
TCP client APIs, not WebSocket encoding.

The inspected JavaScript package is `amps` version
`5.3.4-0.378635.60b2fcc`, obtained from its
[npm distribution](https://registry.npmjs.org/amps/-/amps-5.3.4-0.378635.60b2fcc.tgz).
The archive SHA-256 is
`0898f4309c4113d69ffdde62bca4dbf203cc13fdc2cb07067cf82f03b0dcad00`.

## Wire contract

After HTTP upgrade, the client sends a text message selecting `text` for JSON
or `binary` for binary message handling. Each outgoing command then consists
of a four-byte little-endian part count in a binary message, a JSON header in
a text message, and any payload in a separate message. The count describes
parts, not byte length. Buffered TCP commands are translated independently.

Server responses have no TCP length prefix or client-side part counter. A
header and its body arrive as separate WebSocket messages. A publication or
OOF with `l=0` has no body; without that indication, the adapter reads the next
message as the body even if its bytes resemble a command header.

SOW uses an outer header and successive record-header/body pairs. Each complete
record is delivered before waiting for the next. A disconnection during the
second record therefore preserves the first complete record. Missing record
lengths are reconstructed from the actual body for the internal TCP parser.

## Lifecycle and error handling

Initialization remains bound to the caller's context through the mode preface.
The cancellation watcher is detached and joined before a successful connection
is returned. A watcher for an old connection cannot close a later session.

External write deadlines are synchronized underneath Gorilla and TLS. Setting
a deadline can interrupt a blocked write without racing with Gorilla's private
deadline field. Protocol errors are terminal, and a partially emitted command
or broker response is never restarted by a later `Write`.

The ordinary WebSocket tests cover logon, text/binary payloads, fragmentation,
ping, batched and fragmented writes, empty publication/OOF bodies, SOW lengths,
partial SOW failure, cancellation, WS/WSS deadlines and terminal errors. The
Go-to-fakeamps integration checks exact live/SOW delivery and completion. The
independent official JavaScript client also exercises logon, publication,
subscription, SOW and OOF against fakeamps.

## Scope

These checks reproduce and correct the wire incompatibility. They have not
connected to the user's AMPS 5.3.5.60 instance. Server authentication settings,
reverse proxies and any deployment-specific behavior remain unverified there.

JSON-mode outgoing payloads must be valid UTF-8 for text WebSocket messages;
binary mode preserves arbitrary bytes. A transport filter that makes the
entire header opaque prevents the adapter from finding multipart boundaries
and is rejected. Filters that preserve the header and transform the body
remain separable. The SQL monitoring WebSocket keeps its own JSON protocol.
