# sing-xhttp

Xray's XHTTP transport ported to the sing-box V2RayTransport interface.

Scope of this port (intentional subset of the upstream):

| Mode | Status | HTTP version |
|---|---|---|
| `packet-up` | implemented | H1.1 (plaintext) and H2 (TLS) |
| `stream-up` | implemented | H2 (TLS) only |
| `stream-one` | not planned (REALITY-specific) | - |
| `stream-down` | not planned (multi-transport split) | - |
| HTTP/3 | not planned (no CDN uses QUIC to origin today) | - |

Why stream-up isn't supported on plaintext H1.1: Go's `net/http` client
buffers chunked request bodies internally, breaking the "never-FIN POST"
requirement. Xray works around this by writing raw HTTP/1.1 to a hijacked
socket (`splithttp/h1_conn.go` + `client.go` HTTP/1 branch). Porting that
is ~300 lines of careful state management and brings no value over running
stream-up over TLS, which is the realistic deployment anyway.

## Wire-level interop with stock Xray

Defaults match Xray's defaults:
- `sessionId` / `seq` are appended to URL path (`<path>/<sid>` for GET,
  `<path>/<sid>/<seq>` for POST)
- uplink payload goes in the request body
- `x_padding` query parameter inside a `Referer:` header
- response carries `X-Padding`, `Content-Type: text/event-stream`,
  `X-Accel-Buffering: no`, `Cache-Control: no-store`
- uplink POST carries `Content-Type: application/grpc` (stream-up only)

This means a sing-box client built with `sing-xhttp` should talk to a
stock Xray server (`xhttp` transport, default config) and vice versa.

## Build into sing-box

This library can't be plugged into vanilla sing-box because
`transport/v2ray/transport.go` is a hard-coded switch. We provide a small
patch that adds an external registration hook.

1. Apply `patches/0001-sing-box-register-xhttp-transport.patch` to your
   sing-box checkout.
2. In your custom main package:

```go
import (
    _ "github.com/exedev/sing-xhttp/xhttp" // import for side effects
    "github.com/sagernet/sing-box/transport/v2ray"
    "github.com/exedev/sing-xhttp/xhttp"
)

func init() {
    v2ray.RegisterXHTTP(xhttp.ServerConstructor, xhttp.ClientConstructor)
}
```

## Config example

Client/server outbound + inbound JSON snippet:

```jsonc
"transport": {
  "type": "xhttp",
  "mode": "packet-up",       // or "stream-up"
  "path": "/xhttp",
  "host": "example.com",
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

Note that the `tokenish` method generates base62 strings whose HPACK
Huffman-encoded byte length targets `x_padding_bytes` (i.e. the *wire*
length after HPACK compression matches the configured range).

## Tuning

- `sc_max_each_post_bytes` (default 1 MB) caps the size of each uplink
  POST in `packet-up` mode. Smaller values mean more POSTs per MB.
- `sc_max_buffered_posts` (default 30) limits how many out-of-order POSTs
  the server holds before EOF-ing the session. **On H1.1 the connection
  pool serializes POSTs**, so combinations like `sc_max_each_post_bytes=8192`
  with a 4 MB write quickly produce 500+ in-flight POSTs and stall on the
  default `sc_max_buffered_posts=30`. Either raise the buffered-post
  cap or keep the per-post chunk near 1 MB. H2 multiplexes so the cap
  matters less in practice.
- Both client and server must agree on `sc_max_each_post_bytes` — the server
  rejects oversized POSTs.

## TODO

- [ ] XMUX (multiple H2 connections, request-count limits)
- [ ] stream-up reverse heartbeat tuning (currently a literal port)
- [ ] HTTP/1.1 raw-socket path for stream-up (only if there's demand)
- [ ] uplink-data placement (header / cookie carrying payload) — niche,
      forces tiny chunk sizes; ask if you actually need it
