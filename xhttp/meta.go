package xhttp

import (
	"crypto/rand"
	"math"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/net/http2/hpack"
)

// codec encapsulates all wire-format choices: where to put sessionId / seq, and
// how / where to place the x_padding noise. Built once at New(Client|Server)
// time from Options. Used by both client (apply) and server (extract).
type codec struct {
	basePath string

	sessionPlacement string
	sessionKey       string
	seqPlacement     string
	seqKey           string

	xpadObfs      bool
	xpadPlacement string // when obfs: query | header | cookie. else effectively "query-in-header" (Referer / x_padding).
	xpadKey       string
	xpadHeader    string
	xpadMethod    string
	xpadRange     Range
}

func newCodec(o Options) *codec {
	c := &codec{
		basePath:         normalizePath(o.Path),
		sessionPlacement: o.SessionPlacement,
		sessionKey:       o.SessionKey,
		seqPlacement:     o.SeqPlacement,
		seqKey:           o.SeqKey,
		xpadObfs:         o.XPaddingObfsMode,
		xpadPlacement:    o.XPaddingPlacement,
		xpadKey:          o.XPaddingKey,
		xpadHeader:       o.XPaddingHeader,
		xpadMethod:       o.XPaddingMethod,
		xpadRange:        o.XPaddingBytes.orDefault(100, 1000),
	}
	if c.sessionPlacement == "" {
		c.sessionPlacement = PlacementPath
	}
	if c.seqPlacement == "" {
		c.seqPlacement = PlacementPath
	}
	c.sessionKey = defaultKey(c.sessionKey, c.sessionPlacement, "X-Session", "x_session")
	c.seqKey = defaultKey(c.seqKey, c.seqPlacement, "X-Seq", "x_seq")
	if c.xpadObfs {
		if c.xpadPlacement == "" {
			c.xpadPlacement = PlacementHeader
		}
		if c.xpadKey == "" {
			c.xpadKey = "x_padding"
		}
		if c.xpadHeader == "" {
			c.xpadHeader = "X-Padding"
		}
		if c.xpadMethod == "" {
			c.xpadMethod = PaddingMethodRepeatX
		}
	}
	return c
}

func defaultKey(set, placement, header, querycookie string) string {
	if set != "" {
		return set
	}
	switch placement {
	case PlacementHeader:
		return header
	case PlacementQuery, PlacementCookie:
		return querycookie
	}
	return ""
}

// --- client side: applyToRequest ------------------------------------------

// applyMetaToRequest writes sessionId/seq into the request per configured placement.
// seqStr may be "" (download GET / stream-up POST).
func (c *codec) applyMetaToRequest(req *http.Request, sessionID, seqStr string) {
	if sessionID != "" {
		applyField(req, c.sessionPlacement, c.sessionKey, sessionID)
	}
	if seqStr != "" {
		applyField(req, c.seqPlacement, c.seqKey, seqStr)
	}
}

func applyField(req *http.Request, placement, key, value string) {
	switch placement {
	case PlacementPath:
		req.URL.Path = appendToPath(req.URL.Path, value)
	case PlacementQuery:
		q := req.URL.Query()
		q.Set(key, value)
		req.URL.RawQuery = q.Encode()
	case PlacementHeader:
		req.Header.Set(key, value)
	case PlacementCookie:
		req.AddCookie(&http.Cookie{Name: key, Value: value, Path: "/"})
	}
}

func appendToPath(p, seg string) string {
	if seg == "" {
		return p
	}
	if strings.HasSuffix(p, "/") {
		return p + seg
	}
	return p + "/" + seg
}

// applyPaddingToRequest writes x_padding into the request per configured placement.
func (c *codec) applyPaddingToRequest(req *http.Request) {
	n := int(rangeRand(c.xpadRange))
	if n <= 0 {
		return
	}
	if !c.xpadObfs {
		// Xray default: query-in-header with Referer / x_padding=...
		ref := *req.URL
		q := ref.Query()
		q.Set("x_padding", generatePadding(PaddingMethodRepeatX, n))
		ref.RawQuery = q.Encode()
		req.Header.Set("Referer", ref.String())
		return
	}
	value := generatePadding(c.xpadMethod, n)
	switch c.xpadPlacement {
	case PlacementHeader:
		req.Header.Set(c.xpadHeader, value)
	case PlacementQuery:
		q := req.URL.Query()
		q.Set(c.xpadKey, value)
		req.URL.RawQuery = q.Encode()
	case PlacementCookie:
		req.AddCookie(&http.Cookie{Name: c.xpadKey, Value: value, Path: "/"})
	case PlacementQueryInHeader:
		// not normally selected for obfs, but supported for completeness
		ref := *req.URL
		q := ref.Query()
		q.Set(c.xpadKey, value)
		ref.RawQuery = q.Encode()
		req.Header.Set(c.xpadHeader, ref.String())
	}
}

// applyPaddingToResponseHeader writes server-side x_padding into a response.
// Servers default to header placement; under obfs mode the configured header
// placement is honored (cookie/header/query). We don't emit query padding on
// responses because there is no response URL.
func (c *codec) applyPaddingToResponseHeader(w http.ResponseWriter) {
	n := int(rangeRand(c.xpadRange))
	if n <= 0 {
		return
	}
	if !c.xpadObfs {
		w.Header().Set("X-Padding", generatePadding(PaddingMethodRepeatX, n))
		return
	}
	value := generatePadding(c.xpadMethod, n)
	switch c.xpadPlacement {
	case PlacementCookie:
		http.SetCookie(w, &http.Cookie{Name: c.xpadKey, Value: value, Path: "/"})
	case PlacementHeader, PlacementQueryInHeader, PlacementQuery: // query → fall back to header on response
		w.Header().Set(c.xpadHeader, value)
	}
}

// --- server side: extractFromRequest --------------------------------------

// extractMetaFromRequest returns (sessionID, seqStr, pathMatched). pathMatched
// is false iff the request's path doesn't start with the configured base.
func (c *codec) extractMetaFromRequest(r *http.Request) (sessionID, seqStr string, pathMatched bool) {
	if !strings.HasPrefix(r.URL.Path, c.basePath) {
		return "", "", false
	}

	var pathSegs []string
	if c.sessionPlacement == PlacementPath || c.seqPlacement == PlacementPath {
		tail := strings.TrimPrefix(r.URL.Path, c.basePath)
		tail = strings.TrimPrefix(tail, "/")
		if tail != "" {
			pathSegs = strings.Split(tail, "/")
		}
	}
	pi := 0
	take := func() string {
		if pi >= len(pathSegs) {
			return ""
		}
		v := pathSegs[pi]
		pi++
		return v
	}

	sessionID = extractField(r, c.sessionPlacement, c.sessionKey, take)
	seqStr = extractField(r, c.seqPlacement, c.seqKey, take)
	return sessionID, seqStr, true
}

func extractField(r *http.Request, placement, key string, takePath func() string) string {
	switch placement {
	case PlacementPath:
		return takePath()
	case PlacementQuery:
		return r.URL.Query().Get(key)
	case PlacementHeader:
		return r.Header.Get(key)
	case PlacementCookie:
		if ck, err := r.Cookie(key); err == nil {
			return ck.Value
		}
	}
	return ""
}

// --- padding generators ---------------------------------------------------

const charsetBase62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
const avgHuffmanBytesPerCharBase62 = 0.8
const validationTolerance = 2

func generatePadding(method string, length int) string {
	if length <= 0 {
		return ""
	}
	switch method {
	case PaddingMethodTokenish:
		if s := generateTokenishBase62(length); s != "" {
			return s
		}
		fallthrough
	default:
		return strings.Repeat("X", length)
	}
}

// generateTokenishBase62 produces a random base62 string whose HPACK Huffman
// encoded byte length is within ±validationTolerance of targetHuffmanBytes.
// Ported from Xray-core/transport/internet/splithttp/xpadding.go.
func generateTokenishBase62(targetHuffmanBytes int) string {
	n := int(math.Ceil(float64(targetHuffmanBytes) / avgHuffmanBytesPerCharBase62))
	if n < 1 {
		n = 1
	}
	s, ok := randStringFromCharset(n, charsetBase62)
	if !ok {
		return ""
	}
	const maxIter = 150
	adjust := byte('X')
	for i := 0; i < maxIter; i++ {
		cur := int(hpack.HuffmanEncodeLength(s))
		diff := cur - targetHuffmanBytes
		if diff < 0 {
			if -diff <= validationTolerance {
				return s
			}
			s += string(adjust)
			if adjust == 'X' {
				adjust = 'Z'
			} else {
				adjust = 'X'
			}
		} else {
			if diff <= validationTolerance {
				return s
			}
			if len(s) <= 1 {
				return s
			}
			s = s[:len(s)-1]
		}
	}
	return s
}

func randStringFromCharset(n int, charset string) (string, bool) {
	if n <= 0 || len(charset) == 0 {
		return "", false
	}
	m := len(charset)
	limit := byte(256 - (256 % m))
	out := make([]byte, n)
	i := 0
	buf := make([]byte, 256)
	for i < n {
		if _, err := rand.Read(buf); err != nil {
			return "", false
		}
		for _, b := range buf {
			if b >= limit {
				continue
			}
			out[i] = charset[int(b)%m]
			i++
			if i == n {
				break
			}
		}
	}
	return string(out), true
}

// --- helpers --------------------------------------------------------------

// buildBaseURL returns the configured base path as a URL.URL relative root.
// Path-placement fields will append onto this; non-path placements leave the
// path equal to basePath.
var _ = url.URL{} // keep import even when unused after refactor

// normalizePath matches Xray's GetNormalizedPath: ensures leading and trailing slash.
func normalizePath(p string) string {
	if idx := strings.IndexByte(p, '?'); idx >= 0 {
		p = p[:idx]
	}
	if p == "" || p[0] != '/' {
		p = "/" + p
	}
	if p[len(p)-1] != '/' {
		p = p + "/"
	}
	return p
}
