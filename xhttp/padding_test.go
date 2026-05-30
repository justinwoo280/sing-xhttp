package xhttp

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// --- extractPaddingFromRequest tests ---

func TestExtractPaddingDefaultReferer(t *testing.T) {
	// Default mode: padding in Referer header's x_padding query param.
	c := newCodec(Options{Path: "/xhttp"})

	req := httptest.NewRequest("GET", "https://example.com/xhttp/sid", nil)
	refURL := &url.URL{Scheme: "https", Host: "example.com", Path: "/xhttp/sid"}
	q := refURL.Query()
	q.Set("x_padding", strings.Repeat("X", 200))
	refURL.RawQuery = q.Encode()
	req.Header.Set("Referer", refURL.String())

	got := c.extractPaddingFromRequest(req)
	if len(got) != 200 {
		t.Fatalf("expected 200 bytes padding, got %d", len(got))
	}
}

func TestExtractPaddingDefaultQueryFallback(t *testing.T) {
	// Default mode: no Referer → fallback to URL query param.
	c := newCodec(Options{Path: "/xhttp"})

	pad := strings.Repeat("X", 150)
	req := httptest.NewRequest("GET", "https://example.com/xhttp/sid?x_padding="+pad, nil)

	got := c.extractPaddingFromRequest(req)
	if got != pad {
		t.Fatalf("expected %q, got %q", pad, got)
	}
}

func TestExtractPaddingDefaultNoPadding(t *testing.T) {
	// Default mode: no Referer and no query param → empty.
	c := newCodec(Options{Path: "/xhttp"})

	req := httptest.NewRequest("GET", "https://example.com/xhttp/sid", nil)
	got := c.extractPaddingFromRequest(req)
	if got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestExtractPaddingObfsHeader(t *testing.T) {
	c := newCodec(Options{
		Path:             "/xhttp",
		XPaddingObfsMode: true,
		XPaddingPlacement: PlacementHeader,
		XPaddingHeader:   "X-Pad",
	})

	pad := strings.Repeat("Z", 300)
	req := httptest.NewRequest("POST", "https://example.com/xhttp/sid/0", nil)
	req.Header.Set("X-Pad", pad)

	got := c.extractPaddingFromRequest(req)
	if got != pad {
		t.Fatalf("expected %d bytes, got %d", len(pad), len(got))
	}
}

func TestExtractPaddingObfsCookie(t *testing.T) {
	c := newCodec(Options{
		Path:             "/xhttp",
		XPaddingObfsMode: true,
		XPaddingPlacement: PlacementCookie,
		XPaddingKey:      "pad",
	})

	pad := strings.Repeat("A", 500)
	req := httptest.NewRequest("POST", "https://example.com/xhttp/sid/0", nil)
	req.AddCookie(&http.Cookie{Name: "pad", Value: pad})

	got := c.extractPaddingFromRequest(req)
	if got != pad {
		t.Fatalf("expected %d bytes, got %d", len(pad), len(got))
	}
}

func TestExtractPaddingObfsQuery(t *testing.T) {
	c := newCodec(Options{
		Path:             "/xhttp",
		XPaddingObfsMode: true,
		XPaddingPlacement: PlacementQuery,
		XPaddingKey:      "qpad",
	})

	pad := strings.Repeat("B", 400)
	req := httptest.NewRequest("GET", "https://example.com/xhttp/sid?qpad="+pad, nil)

	got := c.extractPaddingFromRequest(req)
	if got != pad {
		t.Fatalf("expected %d bytes, got %d", len(pad), len(got))
	}
}

func TestExtractPaddingObfsQueryInHeader(t *testing.T) {
	c := newCodec(Options{
		Path:             "/xhttp",
		XPaddingObfsMode: true,
		XPaddingPlacement: PlacementQueryInHeader,
		XPaddingKey:      "x_padding",
		XPaddingHeader:   "Referer",
	})

	pad := strings.Repeat("C", 250)
	headerURL := "https://example.com/path?x_padding=" + pad
	req := httptest.NewRequest("GET", "https://example.com/xhttp/sid", nil)
	req.Header.Set("Referer", headerURL)

	got := c.extractPaddingFromRequest(req)
	if got != pad {
		t.Fatalf("expected %d bytes, got %d", len(pad), len(got))
	}
}

// --- validatePadding tests ---

func TestValidatePaddingRepeatXValid(t *testing.T) {
	c := newCodec(Options{
		Path:         "/xhttp",
		XPaddingBytes: &Range{From: 100, To: 500},
	})

	// Length 100: lower bound → valid
	if !c.validatePadding(strings.Repeat("X", 100)) {
		t.Fatal("padding of length 100 should be valid")
	}
	// Length 500: upper bound → valid
	if !c.validatePadding(strings.Repeat("X", 500)) {
		t.Fatal("padding of length 500 should be valid")
	}
	// Length 300: mid range → valid
	if !c.validatePadding(strings.Repeat("X", 300)) {
		t.Fatal("padding of length 300 should be valid")
	}
}

func TestValidatePaddingRepeatXTooShort(t *testing.T) {
	c := newCodec(Options{
		Path:         "/xhttp",
		XPaddingBytes: &Range{From: 100, To: 500},
	})

	if c.validatePadding(strings.Repeat("X", 99)) {
		t.Fatal("padding of length 99 should be invalid (below From=100)")
	}
	if c.validatePadding(strings.Repeat("X", 1)) {
		t.Fatal("padding of length 1 should be invalid")
	}
}

func TestValidatePaddingRepeatXTooLong(t *testing.T) {
	c := newCodec(Options{
		Path:         "/xhttp",
		XPaddingBytes: &Range{From: 100, To: 500},
	})

	if c.validatePadding(strings.Repeat("X", 501)) {
		t.Fatal("padding of length 501 should be invalid (above To=500)")
	}
	if c.validatePadding(strings.Repeat("X", 1000)) {
		t.Fatal("padding of length 1000 should be invalid")
	}
}

func TestValidatePaddingEmpty(t *testing.T) {
	c := newCodec(Options{
		Path:         "/xhttp",
		XPaddingBytes: &Range{From: 100, To: 500},
	})

	if c.validatePadding("") {
		t.Fatal("empty padding should always be invalid")
	}
}

func TestValidatePaddingTokenishValid(t *testing.T) {
	c := newCodec(Options{
		Path:             "/xhttp",
		XPaddingObfsMode: true,
		XPaddingMethod:   PaddingMethodTokenish,
		XPaddingBytes:    &Range{From: 100, To: 500},
	})

	// Generate a tokenish padding of ~300 Huffman bytes.
	pad := generateTokenishBase62(300)
	if pad == "" {
		t.Fatal("failed to generate tokenish padding")
	}
	if !c.validatePadding(pad) {
		t.Fatalf("tokenish padding of target 300 Huffman bytes should be valid, got len=%d", len(pad))
	}
}

func TestValidatePaddingTokenishInvalid(t *testing.T) {
	c := newCodec(Options{
		Path:             "/xhttp",
		XPaddingObfsMode: true,
		XPaddingMethod:   PaddingMethodTokenish,
		XPaddingBytes:    &Range{From: 100, To: 200},
	})

	// Very short: 5 characters → ~4 Huffman bytes, way below From=100
	if c.validatePadding("ABCDE") {
		t.Fatal("5-char tokenish should be invalid (Huffman bytes < From)")
	}
	// Very long: 1000 chars → ~800 Huffman bytes, way above To=200
	if c.validatePadding(strings.Repeat("a", 1000)) {
		t.Fatal("1000-char tokenish should be invalid (Huffman bytes > To)")
	}
}

func TestValidatePaddingSkipValidation(t *testing.T) {
	// When xpadRange.To == 0 after codec construction, validation is skipped.
	// This can't happen via normal Options (orDefault replaces {0,0} with
	// {100,1000}), but we test the validatePadding logic directly.
	c := &codec{xpadRange: Range{From: 0, To: 0}}

	if !c.validatePadding("anything") {
		t.Fatal("non-empty padding should pass when To == 0 (skip validation)")
	}
	if c.validatePadding("") {
		t.Fatal("empty padding should still fail even when To == 0")
	}
}

func TestValidatePaddingDefaultRange(t *testing.T) {
	// Default range is {100, 1000} when XPaddingBytes is nil.
	c := newCodec(Options{Path: "/xhttp"})

	if !c.validatePadding(strings.Repeat("X", 100)) {
		t.Fatal("100 should be valid with default range")
	}
	if !c.validatePadding(strings.Repeat("X", 1000)) {
		t.Fatal("1000 should be valid with default range")
	}
	if c.validatePadding(strings.Repeat("X", 99)) {
		t.Fatal("99 should be invalid with default range")
	}
	if c.validatePadding(strings.Repeat("X", 1001)) {
		t.Fatal("1001 should be invalid with default range")
	}
}

// --- integration: round-trip client padding → server validation ---

func TestPaddingRoundTripDefault(t *testing.T) {
	// Simulate what the client produces and verify the server would accept it.
	c := newCodec(Options{
		Path:         "/xhttp",
		XPaddingBytes: &Range{From: 100, To: 500},
	})

	// Build a request as the client would.
	req, _ := http.NewRequest("GET", "https://example.com/xhttp/sid", nil)
	c.applyPaddingToRequest(req)

	// Extract and validate as the server would.
	extracted := c.extractPaddingFromRequest(req)
	if extracted == "" {
		t.Fatal("client should have set padding")
	}
	if !c.validatePadding(extracted) {
		t.Fatalf("padding generated by client should pass server validation, len=%d", len(extracted))
	}
}

func TestPaddingRoundTripObfsHeader(t *testing.T) {
	c := newCodec(Options{
		Path:             "/xhttp",
		XPaddingObfsMode: true,
		XPaddingPlacement: PlacementHeader,
		XPaddingHeader:   "X-Pad",
		XPaddingMethod:   PaddingMethodRepeatX,
		XPaddingBytes:    &Range{From: 200, To: 800},
	})

	req, _ := http.NewRequest("POST", "https://example.com/xhttp/sid/0", nil)
	c.applyPaddingToRequest(req)

	extracted := c.extractPaddingFromRequest(req)
	if extracted == "" {
		t.Fatal("client should have set padding in header")
	}
	if !c.validatePadding(extracted) {
		t.Fatalf("obfs header padding should pass validation, len=%d", len(extracted))
	}
}

func TestPaddingRoundTripObfsCookieTokenish(t *testing.T) {
	c := newCodec(Options{
		Path:             "/xhttp",
		XPaddingObfsMode: true,
		XPaddingPlacement: PlacementCookie,
		XPaddingKey:      "cspad",
		XPaddingMethod:   PaddingMethodTokenish,
		XPaddingBytes:    &Range{From: 100, To: 600},
	})

	req, _ := http.NewRequest("POST", "https://example.com/xhttp/sid/0", nil)
	c.applyPaddingToRequest(req)

	extracted := c.extractPaddingFromRequest(req)
	if extracted == "" {
		t.Fatal("client should have set padding in cookie")
	}
	if !c.validatePadding(extracted) {
		t.Fatalf("obfs cookie tokenish padding should pass validation, len=%d", len(extracted))
	}
}
