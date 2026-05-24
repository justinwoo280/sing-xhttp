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
//   - x_padding (obfsMode=false):  always query-in-header (Referer / x_padding) — Xray default
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

// Options mirrors the subset of Xray splithttp.Config we care about.
// JSON shape kept close to Xray for interop convenience.
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
	// matching stock Xray's default and ensuring out-of-the-box interop.
	XPaddingObfsMode bool   `json:"x_padding_obfs_mode,omitempty"`
	XPaddingPlacement string `json:"x_padding_placement,omitempty"` // query | header | cookie
	XPaddingKey       string `json:"x_padding_key,omitempty"`       // default "x_padding"
	XPaddingHeader    string `json:"x_padding_header,omitempty"`    // default "X-Padding" (used for header placement)
	XPaddingMethod    string `json:"x_padding_method,omitempty"`    // repeat-x (default) | tokenish

	// Session / seq placement (P2). Default = path (Xray default).
	SessionPlacement string `json:"session_placement,omitempty"` // path | query | header | cookie
	SessionKey       string `json:"session_key,omitempty"`       // header/cookie/query name; default per placement
	SeqPlacement     string `json:"seq_placement,omitempty"`     // path | query | header | cookie
	SeqKey           string `json:"seq_key,omitempty"`           // header/cookie/query name; default per placement
}

func (r *Range) orDefault(from, to int32) Range {
	if r == nil || r.To == 0 {
		return Range{From: from, To: to}
	}
	return *r
}
