package xhttp

import "github.com/sagernet/sing/common/json/badoption"

// Mode constants. Only packet-up / stream-up are supported in this port.
const (
	ModePacketUp = "packet-up"
	ModeStreamUp = "stream-up"
)

// Placement constants for sessionId / seq / x_padding.
//
// Not all placements are valid for every field:
//   - sessionId / seq: path, query, header, cookie
//   - x_padding (obfsMode=true):   query, header, cookie
//   - x_padding (obfsMode=false):  always query-in-header (Referer / x_padding) — default mode
const (
	PlacementPath          = "path"
	PlacementQuery         = "query"
	PlacementHeader        = "header"
	PlacementCookie        = "cookie"
	PlacementQueryInHeader = "query-in-header"
)

// Padding method constants.
const (
	PaddingMethodRepeatX  = "repeat-x"
	PaddingMethodTokenish = "tokenish"
)

// Range is a [from, to] inclusive int32 range.
type Range struct {
	From int32 `json:"from,omitempty"`
	To   int32 `json:"to,omitempty"`
}

// Options configures an XHTTP transport session.
type Options struct {
	Mode    string               `json:"mode,omitempty"`           // "packet-up" (default) | "stream-up"
	Host    string               `json:"host,omitempty"`           // request Host header
	Path    string               `json:"path,omitempty"`           // base URL path
	Method  string               `json:"method,omitempty"`         // uplink HTTP method, default POST
	Headers badoption.HTTPHeader `json:"headers,omitempty"`        // extra request/response headers

	NoGRPCHeader bool `json:"no_grpc_header,omitempty"` // disable Content-Type: application/grpc on stream-up POST
	NoSSEHeader  bool `json:"no_sse_header,omitempty"`  // disable Content-Type: text/event-stream on download GET

	XPaddingBytes        *Range `json:"x_padding_bytes,omitempty"`           // default {100,1000}
	ScMaxEachPostBytes   *Range `json:"sc_max_each_post_bytes,omitempty"`    // default {1_000_000,1_000_000}
	ScMaxBufferedPosts   int32  `json:"sc_max_buffered_posts,omitempty"`     // default 30
	ScMinPostsIntervalMs *Range `json:"sc_min_posts_interval_ms,omitempty"`  // default {30,30}
	ScStreamUpServerSecs *Range `json:"sc_stream_up_server_secs,omitempty"`  // default {20,80}

	// Padding obfuscation / placement (P2). When XPaddingObfsMode is false (default), the
	// other XPadding* fields are ignored and padding is written via Referer-with-query —
	// matching the default and ensuring out-of-the-box interop.
	XPaddingObfsMode bool   `json:"x_padding_obfs_mode,omitempty"`
	XPaddingPlacement string `json:"x_padding_placement,omitempty"` // query | header | cookie
	XPaddingKey       string `json:"x_padding_key,omitempty"`       // default "x_padding"
	XPaddingHeader    string `json:"x_padding_header,omitempty"`    // default "X-Padding" (used for header placement)
	XPaddingMethod    string `json:"x_padding_method,omitempty"`    // repeat-x (default) | tokenish

	// Session / seq placement. Default = path.
	SessionPlacement string `json:"session_placement,omitempty"` // path | query | header | cookie
	SessionKey       string `json:"session_key,omitempty"`       // header/cookie/query name; default per placement
	SeqPlacement     string `json:"seq_placement,omitempty"`     // path | query | header | cookie
	SeqKey           string `json:"seq_key,omitempty"`           // header/cookie/query name; default per placement

	// XMUX: pool of independent HTTP transports (each one its own TCP+TLS session).
	// Improves throughput by spreading streams across multiple H2 connections — a
	// single H2 connection is capped by the server's MAX_CONCURRENT_STREAMS (commonly
	// 100 on CDNs) — and rotates connections by time/request count to dodge per-conn
	// limits. When nil, a single connection is used (legacy behavior).
	Xmux *XmuxConfig `json:"xmux,omitempty"`
}

// XmuxConfig configures the HTTP connection pool. All zero == unlimited.
type XmuxConfig struct {
	MaxConcurrency   *Range `json:"max_concurrency,omitempty"`     // max in-flight sessions per connection; 0 = unlimited
	MaxConnections   *Range `json:"max_connections,omitempty"`     // max simultaneous connections; 0 = unlimited
	CMaxReuseTimes   *Range `json:"c_max_reuse_times,omitempty"`   // max times a connection is picked; 0 = unlimited
	HMaxRequestTimes *Range `json:"h_max_request_times,omitempty"` // max sessions served per connection; 0 = unlimited
	HMaxReusableSecs *Range `json:"h_max_reusable_secs,omitempty"` // wall-clock lifetime of a connection in seconds; 0 = unlimited
	HKeepAlivePeriod int32  `json:"h_keep_alive_period,omitempty"` // H2 PING interval in seconds; 0 = 30s default; -1 = disable
}

func (r *Range) orDefault(from, to int32) Range {
	if r == nil || r.To == 0 {
		return Range{From: from, To: to}
	}
	return *r
}
