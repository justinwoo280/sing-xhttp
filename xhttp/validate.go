package xhttp

import (
	"net/http"
	"strings"

	E "github.com/sagernet/sing/common/exceptions"
)

// Validate checks an Options for internally inconsistent or unsupported
// configuration. It is called by NewClient and NewServer before any wire
// activity, so misconfiguration fails fast at startup rather than producing
// confusing runtime errors or silent interop breakage.
//
// Validate does not mutate Options and does not apply defaults; empty fields
// are treated as "use default" and accepted.
func (o Options) Validate() error {
	switch o.Mode {
	case "", ModePacketUp, ModeStreamUp, ModeStreamOne, ModeStreamDown, ModeAuto:
	default:
		return E.New("xhttp: unsupported mode: ", o.Mode)
	}

	if o.GRPCFraming {
		switch o.Mode {
		case ModeStreamUp, ModeStreamOne, ModeAuto:
		default:
			return E.New("xhttp: grpc_framing requires stream-up, stream-one or server auto mode")
		}
		if o.NoGRPCHeader {
			return E.New("xhttp: grpc_framing requires gRPC headers")
		}
		if o.Method != "" && o.Method != http.MethodPost {
			return E.New("xhttp: grpc_framing requires POST")
		}
		if o.UplinkDataPlacement != "" && o.UplinkDataPlacement != PlacementBody {
			return E.New("xhttp: grpc_framing requires body uplink placement")
		}
		// Use a stable /Service/Method path that RPC-aware proxies can route.
		if !validGRPCServicePath(o.Path) {
			return E.New("xhttp: grpc_framing path must name one gRPC service, for example /xhttp")
		}
		if o.XPaddingObfsMode && o.XPaddingPlacement == PlacementQuery {
			return E.New("xhttp: grpc_framing does not support query padding on the RPC path")
		}
	}

	if !validMetaPlacement(o.SessionPlacement) {
		return E.New("xhttp: invalid session_placement: ", o.SessionPlacement)
	}
	if !validMetaPlacement(o.SeqPlacement) {
		return E.New("xhttp: invalid seq_placement: ", o.SeqPlacement)
	}

	// A non-path placement carrying both session and seq under the same key
	// would overwrite one with the other. Path placement is positional, so
	// duplicates there are fine. Empty placement defaults to path.
	sessionPlc := orPath(o.SessionPlacement)
	seqPlc := orPath(o.SeqPlacement)
	if sessionPlc != PlacementPath && sessionPlc == seqPlc {
		sk := defaultKey(o.SessionKey, sessionPlc, "X-Session", "x_session")
		qk := defaultKey(o.SeqKey, seqPlc, "X-Seq", "x_seq")
		if sk == qk {
			return E.New("xhttp: session and seq share placement ", sessionPlc, " and key ", sk)
		}
	}

	if o.XPaddingObfsMode {
		if !validPaddingPlacement(o.XPaddingPlacement) {
			return E.New("xhttp: invalid x_padding_placement: ", o.XPaddingPlacement)
		}
		switch o.XPaddingMethod {
		case "", PaddingMethodRepeatX, PaddingMethodTokenish:
		default:
			return E.New("xhttp: invalid x_padding_method: ", o.XPaddingMethod)
		}
	}

	if !validUplinkDataPlacement(o.UplinkDataPlacement) {
		return E.New("xhttp: invalid uplink_data_placement: ", o.UplinkDataPlacement)
	}
	if o.UplinkDataPlacement == PlacementHeader || o.UplinkDataPlacement == PlacementCookie || o.UplinkDataPlacement == PlacementAuto {
		if o.UplinkDataKey == "" {
			return E.New("xhttp: uplink_data_key is required when uplink_data_placement is header/cookie/auto")
		}
	}
	if err := validRange("uplink_chunk_size", o.UplinkChunkSize); err != nil {
		return err
	}

	if err := validRange("session_id_length", o.SessionIDLength); err != nil {
		return err
	}
	if o.ServerMaxHeaderBytes < 0 {
		return E.New("xhttp: server_max_header_bytes must be >= 0")
	}

	for _, rc := range []struct {
		name string
		r    *Range
	}{
		{"x_padding_bytes", o.XPaddingBytes},
		{"sc_max_each_post_bytes", o.ScMaxEachPostBytes},
		{"sc_min_posts_interval_ms", o.ScMinPostsIntervalMs},
		{"sc_stream_up_server_secs", o.ScStreamUpServerSecs},
	} {
		if err := validRange(rc.name, rc.r); err != nil {
			return err
		}
	}
	if o.ScMaxBufferedPosts < 0 {
		return E.New("xhttp: sc_max_buffered_posts must be >= 0")
	}

	if o.Xmux != nil {
		for _, rc := range []struct {
			name string
			r    *Range
		}{
			{"xmux.max_concurrency", o.Xmux.MaxConcurrency},
			{"xmux.max_connections", o.Xmux.MaxConnections},
			{"xmux.c_max_reuse_times", o.Xmux.CMaxReuseTimes},
			{"xmux.h_max_request_times", o.Xmux.HMaxRequestTimes},
			{"xmux.h_max_reusable_secs", o.Xmux.HMaxReusableSecs},
		} {
			if err := validRange(rc.name, rc.r); err != nil {
				return err
			}
		}
	}

	return nil
}

func validGRPCServicePath(path string) bool {
	if path == "" || path == "/" {
		return true
	}
	service := strings.TrimSuffix(strings.TrimPrefix(path, "/"), "/")
	for _, part := range strings.Split(service, ".") {
		if part == "" {
			return false
		}
		for i, ch := range part {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_' || i > 0 && ch >= '0' && ch <= '9') {
				return false
			}
		}
	}
	return true
}

// validMetaPlacement reports whether p is a legal placement for session/seq.
// validMetaPlacement reports whether p is a legal placement for session/seq.
// Empty means "default" (path) and is accepted.
func validMetaPlacement(p string) bool {
	switch p {
	case "", PlacementPath, PlacementQuery, PlacementHeader, PlacementCookie:
		return true
	}
	return false
}

// validPaddingPlacement reports whether p is a legal placement for x_padding
// under obfs mode. Empty means "default" (header) and is accepted.
func validPaddingPlacement(p string) bool {
	switch p {
	case "", PlacementQuery, PlacementHeader, PlacementCookie, PlacementQueryInHeader:
		return true
	}
	return false
}

// validUplinkDataPlacement reports whether p is a legal placement for uplink
// data. Empty means "default" (body) and is accepted.
func validUplinkDataPlacement(p string) bool {
	switch p {
	case "", PlacementBody, PlacementHeader, PlacementCookie, PlacementAuto:
		return true
	}
	return false
}

// validRange checks a nil-able range: From must be non-negative and, unless To
// is zero (meaning "unset / use default"), To must be >= From.
func validRange(name string, r *Range) error {
	if r == nil {
		return nil
	}
	if r.From < 0 || r.To < 0 {
		return E.New("xhttp: ", name, " must be non-negative")
	}
	if r.To != 0 && r.To < r.From {
		return E.New("xhttp: ", name, " has To < From")
	}
	return nil
}

// orPath returns p, treating empty as the default path placement.
func orPath(p string) string {
	if p == "" {
		return PlacementPath
	}
	return p
}
