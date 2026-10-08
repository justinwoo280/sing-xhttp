package xhttp

import "github.com/sagernet/sing/common/json/badoption"

// Mode constants.
const (
	ModePacketUp   = "packet-up"
	ModeStreamUp   = "stream-up"
	ModeStreamOne  = "stream-one"
	ModeStreamDown = "stream-down"
	ModeAuto       = "auto"
)

// Placement constants for sessionId / seq / x_padding / uplink data.
//
// Not all placements are valid for every field:
//   - sessionId / seq: path, query, header, cookie
//   - x_padding (obfsMode=true):   query, header, cookie
//   - x_padding (obfsMode=false):  always query-in-header (Referer / x_padding) — default mode
//   - uplink data: body, header, cookie, auto
const (
	PlacementPath          = "path"
	PlacementQuery         = "query"
	PlacementHeader        = "header"
	PlacementCookie        = "cookie"
	PlacementQueryInHeader = "query-in-header"
	PlacementBody          = "body"
	PlacementAuto          = "auto"
)

// Predefined character sets for custom session ID generation.
var PredefinedTable = map[string]string{
	"ALPHABET": "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
	"Alphabet": "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
	"BASE36":   "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
	"Base62":   "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
	"HEX":      "0123456789ABCDEF",
	"alphabet": "abcdefghijklmnopqrstuvwxyz",
	"base36":   "0123456789abcdefghijklmnopqrstuvwxyz",
	"hex":      "0123456789abcdef",
	"number":   "0123456789",
}

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
	Mode    string               `json:"mode,omitempty"`    // "packet-up" (default) | "stream-up"
	Host    string               `json:"host,omitempty"`    // request Host header
	Path    string               `json:"path,omitempty"`    // base URL path
	Method  string               `json:"method,omitempty"`  // uplink HTTP method, default POST
	Headers badoption.HTTPHeader `json:"headers,omitempty"` // extra request/response headers

	NoGRPCHeader bool `json:"no_grpc_header,omitempty"` // disable Content-Type: application/grpc on stream-up POST
	NoSSEHeader  bool `json:"no_sse_header,omitempty"`  // disable Content-Type: text/event-stream on download GET

	// GRPCFraming uses gRPC messages containing protobuf bytes field 1 for
	// stream-up uploads and both directions of stream-one. Both peers must
	// enable it; stock Xray uses unframed bodies. The RPC is <path>/Tun, with
	// stream-up's session ID in X-Xhttp-Session. Its download GET is unchanged.
	GRPCFraming bool `json:"grpc_framing,omitempty"`

	XPaddingBytes        *Range `json:"x_padding_bytes,omitempty"`          // default {100,1000}
	ScMaxEachPostBytes   *Range `json:"sc_max_each_post_bytes,omitempty"`   // default {1_000_000,1_000_000}
	ScMaxBufferedPosts   int32  `json:"sc_max_buffered_posts,omitempty"`    // default 30
	ScMinPostsIntervalMs *Range `json:"sc_min_posts_interval_ms,omitempty"` // default {30,30}
	ScStreamUpServerSecs *Range `json:"sc_stream_up_server_secs,omitempty"` // default {20,80}

	// Padding obfuscation / placement (P2). When XPaddingObfsMode is false (default), the
	// other XPadding* fields are ignored and padding is written via Referer-with-query —
	// matching the default and ensuring out-of-the-box interop.
	XPaddingObfsMode  bool   `json:"x_padding_obfs_mode,omitempty"`
	XPaddingPlacement string `json:"x_padding_placement,omitempty"` // query | header | cookie
	XPaddingKey       string `json:"x_padding_key,omitempty"`       // default "x_padding"
	XPaddingHeader    string `json:"x_padding_header,omitempty"`    // default "X-Padding" (used for header placement)
	XPaddingMethod    string `json:"x_padding_method,omitempty"`    // repeat-x (default) | tokenish

	// Session / seq placement. Default = path.
	SessionPlacement string `json:"session_placement,omitempty"` // path | query | header | cookie
	SessionKey       string `json:"session_key,omitempty"`       // header/cookie/query name; default per placement
	SeqPlacement     string `json:"seq_placement,omitempty"`     // path | query | header | cookie
	SeqKey           string `json:"seq_key,omitempty"`           // header/cookie/query name; default per placement

	// Uplink data placement: where to put uplink payload.
	// "body" (default) — payload in request body.
	// "header" — payload base64-encoded in headers <key>-0, <key>-1, ...
	// "cookie" — payload base64-encoded in cookies <key>_0, <key>_1, ...
	// "auto"   — try header + cookie + body, concatenate results.
	UplinkDataPlacement string `json:"uplink_data_placement,omitempty"`
	UplinkDataKey       string `json:"uplink_data_key,omitempty"`
	UplinkChunkSize     *Range `json:"uplink_chunk_size,omitempty"`

	// Custom session ID generation. When SessionIDTable is set and
	// SessionIDLength > 0, the client generates a random string from the
	// given character table instead of a UUID. SessionIDTable may be a
	// predefined name (see PredefinedTable) or a literal charset.
	SessionIDTable  string `json:"session_id_table,omitempty"`
	SessionIDLength *Range `json:"session_id_length,omitempty"`

	// ServerMaxHeaderBytes limits the size of HTTP request headers accepted
	// by the server. Default 8192 (http.DefaultMaxHeaderBytes).
	ServerMaxHeaderBytes int32 `json:"server_max_header_bytes,omitempty"`

	// DownloadSettings configures a separate download transport for stream-down
	// mode. When set, the client's GET (download) goes to a different path/host
	// than the POST (upload). The dialer and TLS are shared with the main
	// transport; for a completely separate transport (different TLS/dialer),
	// use NewClientWithDownload.
	DownloadSettings *DownloadConfig `json:"download_settings,omitempty"`

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

// DownloadConfig configures a separate download transport for stream-down mode.
type DownloadConfig struct {
	Host string `json:"host,omitempty"`
	Path string `json:"path,omitempty"`
}

func (r *Range) orDefault(from, to int32) Range {
	if r == nil || r.To == 0 {
		return Range{From: from, To: to}
	}
	return *r
}

// orModeDefault is like orDefault but takes a per-mode default Range.
func (r *Range) orModeDefault(d Range) Range {
	if r == nil || r.To == 0 {
		return d
	}
	return *r
}

// modeDefaults holds the default tuning values. These match stock Xray
// splithttp defaults for maximum interop and predictable behavior:
//   - sc_max_each_post_bytes {1MB,1MB}: fixed 1 MB uplink POST chunk. Fewer,
//     larger POSTs is the single biggest throughput lever and minimizes
//     per-POST round trips through CDNs — a differentiated smaller value was
//     tried and reverted as a net negative over Cloudflare.
//   - sc_min_posts_interval_ms {30,30}: 30 ms anti-burst pacing.
//   - sc_max_buffered_posts 30: server-side reorder buffer.
//   - sc_stream_up_server_secs {20,80}: stream-up heartbeat window.
//   - x_padding_bytes {100,1000}: padding size.
//
// The struct/function are kept per-mode-shaped so a future profile can
// differentiate again without touching call sites; today every mode returns
// the same Xray-aligned values.
type modeDefaults struct {
	maxEachPostBytes   Range
	minPostsIntervalMs Range
	maxBufferedPosts   int32
	streamUpServerSecs Range
	xPaddingBytes      Range
}

func defaultsForMode(mode string) modeDefaults {
	return modeDefaults{
		maxEachPostBytes:   Range{From: 1_000_000, To: 1_000_000},
		minPostsIntervalMs: Range{From: 30, To: 30},
		maxBufferedPosts:   30,
		streamUpServerSecs: Range{From: 20, To: 80},
		xPaddingBytes:      Range{From: 100, To: 1000},
	}
}

// defaultUplinkChunkSize returns the chunk size for header/cookie payload
// placement when UplinkChunkSize is not explicitly configured.
func defaultUplinkChunkSize(placement string, scMaxEachPost Range) Range {
	switch placement {
	case PlacementCookie:
		return Range{From: 2 * 1024, To: 3 * 1024}
	case PlacementHeader:
		return Range{From: 3 * 1000, To: 4 * 1000}
	default:
		return scMaxEachPost
	}
}
