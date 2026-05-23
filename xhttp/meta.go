package xhttp

import (
	"net/http"
	"net/url"
	"strings"
)

// First-phase port: only the default Xray placement is supported, namely:
//   sessionId -> appended to URL path
//   seq       -> appended to URL path
//   payload   -> request body
//   x_padding -> Referer query parameter "x_padding"
//
// This matches Xray's defaults (sessionPlacement/seqPlacement/uplinkDataPlacement
// all unset, xPaddingObfsMode=false), so wire-level interop with stock Xray works.

// buildPath returns basePath joined with the given segments, ensuring exactly
// one separator between each. basePath is expected to start with '/'.
func buildPath(basePath string, segments ...string) string {
	p := strings.TrimRight(basePath, "/")
	for _, seg := range segments {
		if seg == "" {
			continue
		}
		p += "/" + seg
	}
	return p
}

// extractMeta parses the trailing sessionId/seq from request.URL.Path, given
// the configured base path. Returns ("", "", false) if path doesn't match base.
//
// seqStr is "" for the download GET request.
func extractMeta(reqPath, basePath string) (sessionID, seqStr string, ok bool) {
	if !strings.HasPrefix(reqPath, basePath) {
		return "", "", false
	}
	tail := strings.TrimPrefix(reqPath, basePath)
	tail = strings.TrimPrefix(tail, "/")
	if tail == "" {
		return "", "", true
	}
	parts := strings.SplitN(tail, "/", 2)
	sessionID = parts[0]
	if len(parts) > 1 {
		seqStr = parts[1]
	}
	return sessionID, seqStr, true
}

// applyPaddingViaReferer puts an x_padding=XXXX query into the Referer header.
// This matches Xray's default (XPaddingObfsMode=false) behavior.
func applyPaddingViaReferer(h http.Header, reqURL *url.URL, n int) {
	if n <= 0 || reqURL == nil {
		return
	}
	refURL := *reqURL
	q := refURL.Query()
	q.Set("x_padding", padding(n))
	refURL.RawQuery = q.Encode()
	h.Set("Referer", refURL.String())
}

// applyPaddingViaHeader puts X-Padding: XXXX directly in headers; used on
// the server side for responses (matches Xray hub.go default).
func applyPaddingViaHeader(h http.Header, n int) {
	if n <= 0 {
		return
	}
	h.Set("X-Padding", padding(n))
}
