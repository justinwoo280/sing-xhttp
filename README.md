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

## TODO

- [ ] padding placement modes: query / header / cookie (currently only
      Referer-with-query, matching Xray default)
- [ ] `tokenish` padding (HPACK Huffman length-target iteration)
- [ ] XMUX (multiple H2 connections, request-count limits)
- [ ] stream-up reverse heartbeat tuning (currently a literal port)
- [ ] HTTP/1.1 raw-socket path for stream-up (only if there's demand)
