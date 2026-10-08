# sing-xhttp

Xray's XHTTP transport ported to the sing-box V2RayTransport interface.

Scope of this port (intentional subset of the upstream):

| Mode | Status | HTTP version |
|---|---|---|
| `packet-up` | implemented | H1.1 (plaintext, raw-socket pool), H2, and H3 |
| `stream-up` | implemented | H2 and H3 (TLS) |
| `stream-one` | implemented | H2 and H3 (TLS) (REALITY-style single bidirectional stream) |
| `stream-down` | implemented | H1.1 / H2 / H3 (separate download path/host) |
| `auto` | implemented | defaults to packet-up, or stream-one if REALITY; H2/H3 with TLS |
| HTTP/3 | implemented | QUIC transport (client + server), standard TLS only |

HTTP/3 is selected by putting `h3` first in the TLS ALPN list, and requires a
TLS implementation that can expose a standard `*tls.Config`. quic-go performs
the TLS 1.3 handshake itself through `crypto/tls` and exposes no hook for a
caller-supplied ClientHello, so uTLS cannot shape a QUIC handshake — and
REALITY, which is built on uTLS, only speaks HTTP/1.1 or HTTP/2. Asking for
`h3` together with either is rejected at client construction rather than
silently falling back to HTTP/2 over TCP.

Why stream-up isn't supported on plaintext H1.1: Go's `net/http` client
buffers chunked request bodies internally, breaking the "never-FIN POST"
requirement. Xray works around this by writing raw HTTP/1.1 to a hijacked
socket (`splithttp/h1_conn.go` + `client.go` HTTP/1.1 branch). This port
includes `h1_conn.go` for reliable HTTP/1.1 packet-up POSTs (raw-socket
pool matching Xray), but stream-up over H1.1 is still not supported since
the body is a streaming pipe that cannot be serialized to a buffer.

## Wire-level interop with stock Xray

Wire-format defaults match Xray's defaults (placement, headers, content
types — everything the server actually parses):
- `sessionId` / `seq` are appended to URL path (`<path>/<sid>` for GET,
  `<path>/<sid>/<seq>` for POST)
- uplink payload goes in the request body
- `x_padding` query parameter inside a `Referer:` header
- response carries `X-Padding`, `Content-Type: text/event-stream`,
  `X-Accel-Buffering: no`, `Cache-Control: no-store`
- uplink POST carries `Content-Type: application/grpc` (stream-up only)

This means a sing-box client built with `sing-xhttp` should talk to a
stock Xray server (`xhttp` transport, default config) and vice versa.

Local tuning defaults (POST sizing / pacing) also match Xray's — see
[Defaults](#defaults).

### gRPC framing for streaming proxies

Stock XHTTP sends raw bytes even when the upload advertises
`Content-Type: application/grpc`. This works with some HTTP streaming proxies,
but is not a gRPC message stream. A proxy that decodes gRPC messages cannot
forward that body as an RPC. The default raw format remains compatible with
stock Xray.

For a proxy that requires gRPC, enable `grpc_framing` on **both sing-xhttp
endpoints** and select `stream-up` or `stream-one`:

```json
{
  "mode": "stream-up",
  "path": "/xhttp",
  "grpc_framing": true
}
```

This is a sing-xhttp extension and cannot communicate with an unmodified Xray
streaming endpoint. Embedding applications must expose `Options.GRPCFraming`
before the JSON option is available in their own configuration.

- Each streaming POST targets `/xhttp/Tun` (in general, `/<service>/Tun`) and
  sends `Content-Type: application/grpc` and `TE: trailers`. `path` names one
  service, such as `/example.Tunnel`; the empty path uses `/xhttp/Tun`.
- Bodies use the five-byte gRPC header and a protobuf message with
  `bytes data = 1`, matching gRPC-lite's schema. Writes are split into at most
  64 KiB of payload per message; received messages are limited to 4 MiB.
  Compression is unsupported. Stream-one responses and stream-up reverse
  heartbeats use the same encoding, with `grpc-status` response trailers.
- In `stream-up`, the POST carries its session ID in `X-Xhttp-Session`.
  The download remains an ordinary XHTTP GET using the configured session
  placement. Route that GET through an HTTP streaming proxy and the POST
  through the gRPC proxy. A route that accepts only gRPC cannot serve the GET.
- In `stream-one`, upload and download share one bidirectional RPC. The client
  allows the first write before receiving response headers, including when
  the intermediary waits for the first message before forwarding headers.
- Both peers must use matching framing settings. `no_grpc_header`, non-POST
  uplinks, header/cookie payload placement, query padding and query strings
  in the RPC path are incompatible with this option. A server may use
  `mode: "auto"` to accept both framed streaming modes; a client must resolve
  to a streaming mode.

Tests cover a grpc-go proxy that decodes and re-encodes every message, a
standard grpc-go client, HTTP/3 loopback, empty/malformed messages, cancellation
and trailers. For gRPC middleware, use HTTP/2 on each RPC hop. The same framing
works between sing-xhttp peers over HTTP/3, but middleware HTTP/3 support,
request timeouts and buffering limits depend on the deployment.

## Dependencies

The library has **no sing-box dependency**. Direct imports:

- `github.com/sagernet/sing` — interfaces and utilities (network, metadata, tls, logger)
- `github.com/sagernet/quic-go` — http2 / h2c / hpack and QUIC (HTTP/3)
- `golang.org/x/net` — http2 / h2c / hpack
- `github.com/gofrs/uuid/v5` — session id
- `google.golang.org/protobuf` — protobuf wire parsing for optional gRPC framing
- `google.golang.org/grpc` — interoperability tests only; the transport does not
  use the gRPC runtime

The `ServerTransport` / `ClientTransport` / `ServerHandler` interfaces in
`xhttp/adapter.go` are structurally identical to sing-box's
`adapter.V2RayServerTransport` / `V2RayClientTransport` /
`V2RayServerTransportHandler` — sing-box's concrete types satisfy them
automatically. This means sing-box can import sing-xhttp without a
circular dependency.

## Integrating with sing-box

Vanilla sing-box doesn't know about "xhttp" — its
`transport/v2ray/transport.go` is a hard-coded switch. To integrate this
library, your sing-box fork should:

1. Add `V2RayTransportTypeXHTTP = "xhttp"` to `constant/v2ray.go`.
2. Add a `XHTTPOptions` field plus its JSON marshal/unmarshal cases to
   `option/V2RayTransportOptions` (declare a `V2RayXHTTPOptions` struct
   mirroring `xhttp.Options`).
3. Add `case C.V2RayTransportTypeXHTTP:` branches to both
   `NewServerTransport` and `NewClientTransport` in
   `transport/v2ray/transport.go`, dispatching to a small bridge file
   that calls `xhttp.NewServer` / `xhttp.NewClient` and converts the
   option struct.

A worked example lives in the author's sing-box fork at
`transport/v2ray/xhttp.go`. The bridge is roughly 80 lines.

Because the `xhttp.ServerTransport` / `xhttp.ClientTransport` /
`xhttp.ServerHandler` interfaces are structurally identical to
sing-box's `adapter.V2RayServerTransport` /
`adapter.V2RayClientTransport` /
`adapter.V2RayServerTransportHandler`, sing-box's concrete types satisfy
our interfaces by Go's structural typing — no wrapping required.

## Config example

Client/server outbound + inbound JSON snippet:

```jsonc
"transport": {
  "type": "xhttp",
  "mode": "packet-up",       // or "stream-up" / "stream-one" / "stream-down" / "auto"
  "path": "/xhttp",
  "host": "example.com",
  // optional — omit to use the default ({1MB,1MB}):
  "sc_max_each_post_bytes": { "from": 1000000, "to": 1000000 },
  "x_padding_bytes":        { "from": 100,     "to": 1000 }
}
```

### Placement / padding obfuscation

Both sides must agree on placement choices. Defaults match stock Xray
(everything on the path, padding via `Referer?x_padding=...`).

```jsonc
"transport": {
  "type": "xhttp",
  "path": "/xhttp",
  // session and seq in headers instead of URL path:
  "session_placement": "header",  // path | query | header | cookie
  "session_key":       "X-Sid",   // defaults: "X-Session" / "x_session"
  "seq_placement":     "header",
  "seq_key":           "X-Sq",

  // padding obfuscation (when off, Referer/x_padding is used — Xray default):
  "x_padding_obfs_mode":  true,
  "x_padding_placement": "header",     // query | header | cookie
  "x_padding_header":    "X-Padding",  // for header placement
  "x_padding_key":       "x_padding",  // for query/cookie placement
  "x_padding_method":    "tokenish"    // repeat-x (default) | tokenish
}
```

### Uplink data placement

Payload can be sent in the request body (default), or encoded as base64
chunks in headers / cookies, matching Xray's `uplinkDataPlacement`:

```jsonc
"transport": {
  "type": "xhttp",
  "path": "/xhttp",
  "uplink_data_placement": "header",  // body (default) | header | cookie | auto
  "uplink_data_key":       "payload",  // required for header/cookie/auto
  "uplink_chunk_size":    { "from": 3000, "to": 4000 }  // optional
}
```

### Custom session ID

```jsonc
"transport": {
  "type": "xhttp",
  "session_id_table":  "Base62",                // predefined name or literal charset
  "session_id_length": { "from": 16, "to": 16 } // omit to use UUID v4
}
```

### stream-down (separate download path)

```jsonc
"transport": {
  "type": "xhttp",
  "mode": "stream-down",
  "path": "/xhttp",
  "download_settings": {
    "host": "download.example.com",
    "path": "/dl"
  }
}
```

Note that the `tokenish` method generates base62 strings whose HPACK
Huffman-encoded byte length targets `x_padding_bytes` (i.e. the *wire*
length after HPACK compression matches the configured range).

### XMUX (multiple H2 connections)

A single HTTP/2 connection is capped by the server's `MAX_CONCURRENT_STREAMS`
(commonly 100 on CDNs). XMUX spreads sessions over a pool of independent
connections and rotates them on time / request-count limits to dodge
per-connection caps. Each pool entry is its own TCP+TLS session.

When `xmux` is absent or empty, the client uses a single connection (legacy
behavior, indistinguishable from earlier versions on the wire).

```jsonc
"transport": {
  "type": "xhttp",
  "path": "/xhttp",
  "xmux": {
    "max_concurrency":     { "from": 16,  "to": 16  }, // in-flight sessions per conn; 0 = unlimited
    "max_connections":     { "from": 4,   "to": 4   }, // simultaneous conns; 0 = unlimited
    "c_max_reuse_times":   { "from": 64,  "to": 128 }, // how many times a conn is picked before retiring
    "h_max_request_times": { "from": 600, "to": 900 }, // sessions served per conn before retiring
    "h_max_reusable_secs": { "from": 1800,"to": 3600 } // wall-clock lifetime per conn (seconds)
  }
}
```

The XMUX field names mirror Xray's so the same config block works on both
sides. Setting `max_connections > 0` with `max_concurrency=0` simply spreads
sessions over up to N conns without per-conn caps.

### Chromium-aligned HTTP/2 settings

The H2 client sends the same initial SETTINGS values and initial session
WINDOW_UPDATE that Chromium does, so the connection opening does not stand out
from a browser's. Sources are Chromium `net/spdy/spdy_session.cc`
(`SendInitialData`) and `net/http/http_network_session.cc`
(`AddDefaultHttp2Settings`).

| Setting | Value | Chromium constant |
|---|---|---|
| `0x1` HEADER_TABLE_SIZE | 65536 | `kSpdyMaxHeaderTableSize` |
| `0x2` ENABLE_PUSH | 0 | `kSpdyDisablePush` |
| `0x4` INITIAL_WINDOW_SIZE | 6291456 | `kSpdyStreamMaxRecvWindowSize` |
| `0x6` MAX_HEADER_LIST_SIZE | 262144 | `kSpdyMaxHeaderListSize` |
| WINDOW_UPDATE (stream 0) | 15663105 | `kSpdySessionMaxRecvWindowSize - kDefaultInitialWindowSize` |

Idle connections are never closed on a timer, matching Chromium: `SpdySession`
has no idle timeout and keeps idle sessions indefinitely for reuse.

The client adapts the upstream transport's outbound frame stream without
forking `golang.org/x/net/http2`: the default `0x5` MAX_FRAME_SIZE setting is
removed, the remaining initial settings are emitted in Chromium's order,
request HEADERS carry the Chromium-style priority tuple, and the RFC 9218
`priority: i` header is added for HTTP/2 requests. HPACK header blocks are
decoded and re-encoded in the same client-to-server context so the request
pseudo-headers are ordered as `:method, :authority, :scheme, :path` while
preserving dynamic-table state.

One deliberate departure: Chrome sends no periodic PING (its only liveness
check is a lazy PING emitted just before a write on a connection that has been
read-idle for >10s, which `http2.Transport` cannot express). We keep a 30s
`ReadIdleTimeout` instead, because without it a silently-dead connection is
only noticed when the next POST fails — an upload-idle/download-active session
would hang until the OS TCP timeout. Set `h_keep_alive_period: -1` to disable.

`TestChromeLikeH2InitialFrames` asserts the frames on the wire.

## Defaults

When a tuning field is left unset, these Xray-aligned defaults apply (see
`defaultsForMode`). They are uniform across modes for predictable behavior and
maximum interop. An explicit user value always overrides the default (a range
with `To == 0` is treated as "unset").

| Parameter | Default | Notes |
|---|---|---|
| `sc_max_each_post_bytes`   | `{1MB, 1MB}` | fixed 1 MB uplink POST chunk (packet-up / stream-down) |
| `sc_min_posts_interval_ms` | `{30, 30}` ms | anti-burst pacing between POSTs |
| `sc_max_buffered_posts`    | `30` | server-side reorder buffer |
| `sc_stream_up_server_secs` | `{20, 80}` s | stream-up / stream-one heartbeat window |
| `x_padding_bytes`          | `{100, 1000}` | padding size |

A differentiated per-mode profile (smaller randomized post size for packet-up)
was tried and reverted: over response-buffering CDNs like Cloudflare, a fixed
1 MB post minimizes per-POST round trips and is the single biggest throughput
lever, so matching Xray's defaults is both faster and simpler. The
`defaultsForMode` function keeps its per-mode shape so a future profile can
differentiate again without touching call sites.

These are all **local behavior** parameters (POST sizing / pacing / heartbeat /
reorder buffer). They never change the wire format — the server never inspects
POST timing — so interop with stock Xray is unaffected.

## Tuning

- `sc_max_each_post_bytes` caps the size of each uplink POST in `packet-up` /
  `stream-down` mode. Smaller values mean more POSTs per MB. Default is a
  fixed `{1MB, 1MB}` (see [Defaults](#defaults)).
- `sc_max_buffered_posts` (default 30) limits how many out-of-order POSTs
  the server holds before EOF-ing the session. It guards against a client
  sending seq numbers far ahead of what the server can reassemble. Raising
  it does **not** help small-chunk throughput — the bottleneck there is
  per-POST pacing, not the reorder buffer (verified: 30 vs 256 made no
  difference in the benchmark). H2 multiplexes POSTs so the cap rarely
  bites in practice.
- Both client and server must agree on `sc_max_each_post_bytes` — the server
  rejects oversized POSTs.

### Recommended packet-up settings

`packet-up` is inherently a stream of short, high-frequency POSTs, so it
will never match `stream-up` throughput — don't expect it to. The knobs
below trade obfuscation for speed; pick based on what you need.

- **Throughput-first (default):** keep `sc_max_each_post_bytes` at the fixed
  `{1MB, 1MB}` default and leave `sc_min_posts_interval_ms` at `{30,30}`. Fewer,
  larger POSTs is the single biggest lever (~32 MB/s in the loopback benchmark)
  and minimizes per-POST round trips through CDNs.
- **Obfuscation-first (small chunks):** if you set a small
  `sc_max_each_post_bytes` (e.g. 16 KB) to blend in, the per-POST pacing
  interval dominates and throughput drops sharply, because packet-up sends
  POSTs sequentially (see below).
- **`sc_min_posts_interval_ms`** defaults to `{30, 30}` (30 ms), matching
  Xray. It is a *minimum* interval measured from the previous POST's
  dispatch: if assembling the next chunk already took longer than the
  interval, no extra wait is added. Lowering it raises throughput on small
  chunks at the cost of a more bursty, more fingerprintable POST cadence.
  Setting `{0, 0}` is treated as "unset" and restores the 30 ms default —
  use a small non-zero value like `{1, 1}` if you genuinely want to disable
  pacing.

### Uplink model (packet-up)

The uploader mirrors stock Xray exactly: a single sequential loop drains a
chunk, assigns the next sequence number, then fires the POST in a goroutine
but blocks only until that request's **body has been flushed to the socket**
(`httptrace.WroteRequest`) — not until the response arrives. The 200 OK is
awaited and discarded in the background. This keeps POSTs in strict sequence
order (so the server's reassembly queue never has to buffer/reorder) while
letting response round-trips overlap. An earlier revision used bounded
concurrent round-trips with per-connection pacing; over Cloudflare that
reordered sequence numbers and stacked response latency, cutting uplink
throughput roughly in half — the sequential pipeline restores it.

## Benchmarks

Loopback echo over a self-signed H2 connection on a 13th-gen i5 laptop. These
are relative-comparison numbers (single machine, no real network), not
absolute throughput claims. Reproduce with:

```sh
GOARCH=amd64 go test ./xhttp/ -run='^$' -bench='Throughput|Dial' -benchtime=50x
```

Benchmarks pin `sc_max_each_post_bytes` explicitly (fixed 1 MB or 16 KB) to
isolate the post-size variable, rather than relying on the default.

| Benchmark | Throughput | Notes |
|---|---|---|
| stream-up, 1 MB | ~190 MB/s | single long-lived POST, no per-packet overhead |
| packet-up TLS, 1 MB | ~32 MB/s | fixed 1 MB post chunk |
| packet-up plaintext, 1 MB | ~32 MB/s | H1 pool, comparable to H2 at 1 MB chunk |
| packet-up TLS, 1 MB, 16 KB chunks | ~0.5 MB/s | many small POSTs — pacing-bound |

The 16 KB-chunk row shows the per-POST pacing cost: small
`sc_max_each_post_bytes` means many POSTs, each gated by the sequential
`sc_min_posts_interval_ms`. For real throughput, keep the per-post chunk
near 1 MB (the default), or use stream-up where a single POST carries the
whole uplink.

Over a real Cloudflare-fronted server, the sequential pipeline sustains
~16 MB/s uplink / ~40 MB/s downlink for 10–20 MB transfers (vs ~6 MB/s
uplink on the earlier concurrent-round-trip revision).

CPU micro-benchmarks (`-bench='GeneratePadding|ApplyMeta|ApplyPadding'`) show
the request hot path is cheap (~0.4–0.5 µs to place meta), with default-mode
padding being the most expensive step (~2.7 µs, URL clone + query re-encode)
and `tokenish` padding ~20x costlier than `repeat-x` due to its HPACK-length
feedback loop.

## Options validation

`xhttp.Options.Validate()` runs automatically inside `NewClient` / `NewServer`
and rejects inconsistent configuration before any wire activity: unknown mode,
invalid session/seq/padding placements, a session+seq collision on the same
non-path placement and key, unknown padding method, and malformed ranges
(negative bounds or `To < From`). Empty fields are treated as "use default"
and accepted.

## TODO

- [ ] REALITY integration tests — the library detects REALITY via `Config()`
      error and auto-resolves to `stream-one`, but needs end-to-end testing
      with sing-box's REALITY implementation
- [ ] Fully separate download transport (different TLS/dialer for stream-down)
      — currently only path/host can differ; the dialer/TLS is shared. A
      `NewClientWithDownload` constructor could add this if needed
