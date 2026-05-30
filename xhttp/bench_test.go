package xhttp_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/justinwoo280/sing-xhttp/xhttp"

	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

// benchEnv holds a running server+client pair for throughput benchmarks.
type benchEnv struct {
	server *xhttp.Server
	client *xhttp.Client
	lis    net.Listener
}

func (e *benchEnv) Close() {
	if e.client != nil {
		e.client.Close()
	}
	if e.server != nil {
		e.server.Close()
	}
	if e.lis != nil {
		e.lis.Close()
	}
}

// setupBenchEnv stands up a loopback echo server and a matching client.
func setupBenchEnv(b *testing.B, mode string, useTLS bool, customize func(*xhttp.Options)) *benchEnv {
	b.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	ctx := context.Background()
	opts := xhttp.Options{
		Mode: mode,
		Path: "/xhttp",
	}
	if customize != nil {
		customize(&opts)
	}

	var sCfg = anyServerTLS(nil)
	var cCfg = anyClientTLS(nil)
	if useTLS {
		sTLS, cTLS := makeTLSPair(b)
		sCfg = anyServerTLS(sTLS)
		cCfg = anyClientTLS(cTLS)
	}

	server, err := xhttp.NewServer(ctx, logger.NOP(), opts, sCfg, echoHandler{})
	if err != nil {
		listener.Close()
		b.Fatal(err)
	}
	go server.Serve(listener)
	time.Sleep(50 * time.Millisecond)

	client, err := xhttp.NewClient(ctx, directDialer{}, M.ParseSocksaddrHostPort("127.0.0.1", uint16(port)), opts, cCfg)
	if err != nil {
		server.Close()
		listener.Close()
		b.Fatal(err)
	}

	return &benchEnv{server: server, client: client, lis: listener}
}

// echoRoundTrip writes payload and reads back the same number of bytes,
// verifying integrity. Used as the per-iteration body of throughput benchmarks.
func echoRoundTrip(b *testing.B, conn net.Conn, payload []byte) {
	recv := make([]byte, len(payload))
	var wg sync.WaitGroup
	var readErr, writeErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, readErr = io.ReadFull(conn, recv)
	}()
	go func() {
		defer wg.Done()
		_, writeErr = io.Copy(conn, bytes.NewReader(payload))
	}()
	wg.Wait()
	if readErr != nil {
		b.Fatalf("read: %v", readErr)
	}
	if writeErr != nil {
		b.Fatalf("write: %v", writeErr)
	}
	if !bytes.Equal(payload, recv) {
		b.Fatal("data mismatch")
	}
}

// --- end-to-end throughput benchmarks ---

func benchThroughput(b *testing.B, mode string, useTLS bool, payloadSize int, postChunk int32) {
	env := setupBenchEnv(b, mode, useTLS, func(o *xhttp.Options) {
		if postChunk > 0 {
			o.ScMaxEachPostBytes = &xhttp.Range{From: postChunk, To: postChunk}
		}
	})
	defer env.Close()

	payload := make([]byte, payloadSize)
	rand.Read(payload)

	b.SetBytes(int64(payloadSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn, err := env.client.DialContext(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		echoRoundTrip(b, conn, payload)
		conn.Close()
	}
}

func BenchmarkThroughputPacketUpTLS_64K(b *testing.B) {
	benchThroughput(b, xhttp.ModePacketUp, true, 64*1024, 0)
}

func BenchmarkThroughputPacketUpTLS_1M(b *testing.B) {
	benchThroughput(b, xhttp.ModePacketUp, true, 1024*1024, 0)
}

func BenchmarkThroughputPacketUpTLS_1M_SmallChunks(b *testing.B) {
	// Small per-POST chunk -> many concurrent POSTs per session. Stresses the
	// batch accumulator + concurrent uploader + server reorder queue.
	benchThroughput(b, xhttp.ModePacketUp, true, 1024*1024, 16*1024)
}

// --- isolating the per-POST pacing interval ---
//
// These mirror the two TLS packet-up cases above but disable the default
// 30 ms ScMinPostsIntervalMs gate. Comparing each pair quantifies how much
// throughput the pacing interval costs.

func benchThroughputNoInterval(b *testing.B, payloadSize int, postChunk int32) {
	env := setupBenchEnv(b, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.ScMinPostsIntervalMs = &xhttp.Range{From: 1, To: 1} // ~0; To=0 would mean "use default 30"
		if postChunk > 0 {
			o.ScMaxEachPostBytes = &xhttp.Range{From: postChunk, To: postChunk}
		}
	})
	defer env.Close()

	payload := make([]byte, payloadSize)
	rand.Read(payload)

	b.SetBytes(int64(payloadSize))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := env.client.DialContext(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		echoRoundTrip(b, conn, payload)
		conn.Close()
	}
}

func BenchmarkThroughputPacketUpTLS_1M_NoInterval(b *testing.B) {
	benchThroughputNoInterval(b, 1024*1024, 0)
}

func BenchmarkThroughputPacketUpTLS_1M_SmallChunks_NoInterval(b *testing.B) {
	benchThroughputNoInterval(b, 1024*1024, 16*1024)
}

// --- diagnosing the small-chunk cliff ---
//
// Sweep chunk size and the server-side reorder buffer to locate the stall.
// If throughput tracks chunk count, the cost is per-POST. If raising the
// buffered-post cap rescues small chunks, the stall is the reorder queue.

func benchThroughputTuned(b *testing.B, payloadSize int, postChunk int32, bufferedPosts int32) {
	env := setupBenchEnv(b, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.ScMaxEachPostBytes = &xhttp.Range{From: postChunk, To: postChunk}
		if bufferedPosts > 0 {
			o.ScMaxBufferedPosts = bufferedPosts
		}
	})
	defer env.Close()

	payload := make([]byte, payloadSize)
	rand.Read(payload)

	b.SetBytes(int64(payloadSize))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := env.client.DialContext(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		echoRoundTrip(b, conn, payload)
		conn.Close()
	}
}

func BenchmarkChunk16K_Buf30(b *testing.B)  { benchThroughputTuned(b, 1024*1024, 16*1024, 30) }
func BenchmarkChunk16K_Buf256(b *testing.B) { benchThroughputTuned(b, 1024*1024, 16*1024, 256) }
func BenchmarkChunk64K_Buf30(b *testing.B)  { benchThroughputTuned(b, 1024*1024, 64*1024, 30) }
func BenchmarkChunk256K_Buf30(b *testing.B) { benchThroughputTuned(b, 1024*1024, 256*1024, 30) }

// BenchmarkChunk16K_Xmux4 measures whether spreading a single session over a
// 4-connection xmux pool relieves the small-chunk cliff under current code.
// If it stays ~0.5 MB/s, the session is pinned to one connection and the
// 30 ms interval gates the whole session regardless of pool size.
func BenchmarkChunk16K_Xmux4(b *testing.B) {
	env := setupBenchEnv(b, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.ScMaxEachPostBytes = &xhttp.Range{From: 16 * 1024, To: 16 * 1024}
		o.Xmux = &xhttp.XmuxConfig{
			MaxConnections: &xhttp.Range{From: 4, To: 4},
			MaxConcurrency: &xhttp.Range{From: 4, To: 4},
		}
	})
	defer env.Close()

	payload := make([]byte, 1024*1024)
	rand.Read(payload)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := env.client.DialContext(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		echoRoundTrip(b, conn, payload)
		conn.Close()
	}
}

func BenchmarkThroughputStreamUpTLS_1M(b *testing.B) {
	benchThroughput(b, xhttp.ModeStreamUp, true, 1024*1024, 0)
}

func BenchmarkThroughputPacketUpPlain_1M(b *testing.B) {
	benchThroughput(b, xhttp.ModePacketUp, false, 1024*1024, 0)
}

// --- connection-establishment (dial) benchmark ---

func BenchmarkDialPacketUpTLS(b *testing.B) {
	env := setupBenchEnv(b, xhttp.ModePacketUp, true, nil)
	defer env.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := env.client.DialContext(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		// Drive one byte each way so the session is fully established.
		go conn.Write([]byte{'x'})
		buf := make([]byte, 1)
		_, _ = io.ReadFull(conn, buf)
		conn.Close()
	}
}
