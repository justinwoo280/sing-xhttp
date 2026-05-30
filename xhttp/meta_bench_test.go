package xhttp

import (
	"net/http"
	"testing"
)

// --- padding generation micro-benchmarks ---

func BenchmarkGeneratePaddingRepeatX(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = generatePadding(PaddingMethodRepeatX, 500)
	}
}

func BenchmarkGeneratePaddingTokenish(b *testing.B) {
	// tokenish runs an HPACK-length feedback loop, so it is materially more
	// expensive than repeat-x; this quantifies the gap.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = generatePadding(PaddingMethodTokenish, 500)
	}
}

// --- codec hot-path micro-benchmarks ---

func BenchmarkApplyMetaPath(b *testing.B) {
	c := newCodec(Options{Path: "/xhttp"})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, "https://example.com/xhttp/", nil)
		c.applyMetaToRequest(req, "0123456789abcdef0123456789abcdef", "42")
	}
}

func BenchmarkApplyMetaHeader(b *testing.B) {
	c := newCodec(Options{
		Path:             "/xhttp",
		SessionPlacement: PlacementHeader,
		SeqPlacement:     PlacementHeader,
	})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, "https://example.com/xhttp/", nil)
		req.Header = make(http.Header)
		c.applyMetaToRequest(req, "0123456789abcdef0123456789abcdef", "42")
	}
}

func BenchmarkApplyPaddingDefault(b *testing.B) {
	c := newCodec(Options{Path: "/xhttp", XPaddingBytes: &Range{From: 100, To: 1000}})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, "https://example.com/xhttp/", nil)
		req.Header = make(http.Header)
		c.applyPaddingToRequest(req)
	}
}

func BenchmarkExtractMetaPath(b *testing.B) {
	c := newCodec(Options{Path: "/xhttp"})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPost, "https://example.com/xhttp/0123456789abcdef0123456789abcdef/42", nil)
		_, _, _ = c.extractMetaFromRequest(req)
	}
}

func BenchmarkValidatePaddingRepeatX(b *testing.B) {
	c := newCodec(Options{Path: "/xhttp", XPaddingBytes: &Range{From: 100, To: 1000}})
	pad := generatePadding(PaddingMethodRepeatX, 500)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = c.validatePadding(pad)
	}
}
