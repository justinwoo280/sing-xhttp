package xhttp

import "github.com/sagernet/sing/common/json/badoption"

// Mode constants. Only packet-up / stream-up are supported in this port.
const (
	ModePacketUp = "packet-up"
	ModeStreamUp = "stream-up"
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
}

func (r *Range) orDefault(from, to int32) Range {
	if r == nil || r.To == 0 {
		return Range{From: from, To: to}
	}
	return *r
}
