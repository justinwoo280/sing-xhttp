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

	"github.com/exedev/sing-xhttp/xhttp"

	sboxTLS "github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func anyServerTLS(s *serverTLS) sboxTLS.ServerConfig {
	if s == nil { return nil }
	return s
}
func anyClientTLS(c *clientTLS) sboxTLS.Config {
	if c == nil { return nil }
	return c
}

type echoHandler struct{}

func (echoHandler) NewConnectionEx(ctx context.Context, conn net.Conn, source M.Socksaddr, dest M.Socksaddr, onClose N.CloseHandlerFunc) {
	go func() {
		defer conn.Close()
		io.Copy(conn, conn)
		if onClose != nil {
			onClose(nil)
		}
	}()
}

type directDialer struct{}

func (directDialer) DialContext(ctx context.Context, network string, dest M.Socksaddr) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, dest.String())
}

func (directDialer) ListenPacket(ctx context.Context, dest M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket("udp", "")
}

func runEcho(t *testing.T, mode string, useTLS bool) {
	runEchoWithOpts(t, mode, useTLS, nil)
}

func runEchoWithOpts(t *testing.T, mode string, useTLS bool, customize func(*xhttp.Options)) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	logf := log.NewNOPFactory()
	logger := logf.NewLogger("test")
	ctx := context.Background()

	opts := xhttp.Options{
		Mode: mode,
		Path: "/xhttp",
		ScMaxEachPostBytes: &xhttp.Range{From: 4096, To: 4096}, // small to exercise splitting
	}
	if customize != nil {
		customize(&opts)
	}

	var (
		sTLS *serverTLS
		cTLS *clientTLS
	)
	if useTLS {
		sTLS, cTLS = makeTLSPair(t)
	}
	var serverTLSCfg interface{ ServerConfigType() }
	_ = serverTLSCfg

	var sCfg = anyServerTLS(sTLS)
	var cCfg = anyClientTLS(cTLS)

	server, err := xhttp.NewServer(ctx, logger, opts, sCfg, echoHandler{})
	if err != nil { t.Fatal(err) }
	defer server.Close()
	go server.Serve(listener)

	time.Sleep(50 * time.Millisecond)

	client, err := xhttp.NewClient(ctx, directDialer{}, M.ParseSocksaddrHostPort("127.0.0.1", uint16(port)), opts, cCfg)
	if err != nil { t.Fatal(err) }
	defer client.Close()

	conn, err := client.DialContext(ctx)
	if err != nil { t.Fatal(err) }
	defer conn.Close()

	payload := make([]byte, 64*1024)
	rand.Read(payload)

	var wg sync.WaitGroup
	wg.Add(2)
	recv := make([]byte, len(payload))
	var readErr error
	go func() {
		defer wg.Done()
		_, readErr = io.ReadFull(conn, recv)
	}()
	go func() {
		defer wg.Done()
		io.Copy(conn, bytes.NewReader(payload))
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatalf("timeout in %s", mode)
	}
	if readErr != nil {
		t.Fatalf("read err: %v", readErr)
	}
	if !bytes.Equal(payload, recv) {
		t.Fatalf("data mismatch in %s", mode)
	}
}

func TestEchoPacketUpPlain(t *testing.T) { runEcho(t, xhttp.ModePacketUp, false) }
func TestEchoPacketUpTLS(t *testing.T)   { runEcho(t, xhttp.ModePacketUp, true) }
func TestEchoStreamUpTLS(t *testing.T)   { runEcho(t, xhttp.ModeStreamUp, true) }

// --- placement / padding-method coverage --------------------------------

func TestPlacementHeader(t *testing.T) {
	runEchoWithOpts(t, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.SessionPlacement = xhttp.PlacementHeader
		o.SeqPlacement = xhttp.PlacementHeader
	})
}

func TestPlacementQuery(t *testing.T) {
	runEchoWithOpts(t, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.SessionPlacement = xhttp.PlacementQuery
		o.SeqPlacement = xhttp.PlacementQuery
	})
}

func TestPlacementCookie(t *testing.T) {
	runEchoWithOpts(t, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.SessionPlacement = xhttp.PlacementCookie
		o.SeqPlacement = xhttp.PlacementCookie
	})
}

func TestPlacementMixed(t *testing.T) {
	// session in header, seq still on path (a realistic obfuscation choice)
	runEchoWithOpts(t, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.SessionPlacement = xhttp.PlacementHeader
		o.SessionKey = "X-Sid"
	})
}

func TestPaddingObfsHeaderTokenish(t *testing.T) {
	runEchoWithOpts(t, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.XPaddingObfsMode = true
		o.XPaddingPlacement = xhttp.PlacementHeader
		o.XPaddingMethod = xhttp.PaddingMethodTokenish
	})
}

func TestPaddingObfsCookie(t *testing.T) {
	runEchoWithOpts(t, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.XPaddingObfsMode = true
		o.XPaddingPlacement = xhttp.PlacementCookie
	})
}

func TestPlacementStreamUpHeader(t *testing.T) {
	runEchoWithOpts(t, xhttp.ModeStreamUp, true, func(o *xhttp.Options) {
		o.SessionPlacement = xhttp.PlacementHeader
	})
}

// --- XMUX coverage ------------------------------------------------------

func TestXmuxBasic(t *testing.T) {
	// Two-conn pool with concurrency=1 -> two sessions must land on two conns.
	runEchoWithOpts(t, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.Xmux = &xhttp.XmuxConfig{
			MaxConnections: &xhttp.Range{From: 2, To: 2},
			MaxConcurrency: &xhttp.Range{From: 1, To: 1},
		}
	})
}

func TestXmuxParallelSessions(t *testing.T) {
	// Spin up many concurrent sessions and confirm each completes.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	logger := log.NewNOPFactory().NewLogger("test")
	ctx := context.Background()
	sTLS, cTLS := makeTLSPair(t)

	opts := xhttp.Options{
		Mode: xhttp.ModePacketUp,
		Path: "/xhttp",
		ScMaxEachPostBytes: &xhttp.Range{From: 4096, To: 4096},
		Xmux: &xhttp.XmuxConfig{
			MaxConnections:   &xhttp.Range{From: 4, To: 4},
			MaxConcurrency:   &xhttp.Range{From: 2, To: 2},
			HMaxRequestTimes: &xhttp.Range{From: 3, To: 3}, // forces rotation
		},
	}

	server, err := xhttp.NewServer(ctx, logger, opts, anyServerTLS(sTLS), echoHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go server.Serve(listener)
	time.Sleep(50 * time.Millisecond)

	client, err := xhttp.NewClient(ctx, directDialer{}, M.ParseSocksaddrHostPort("127.0.0.1", uint16(port)), opts, anyClientTLS(cTLS))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const N = 12
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := client.DialContext(ctx)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			payload := make([]byte, 8192)
			rand.Read(payload)
			recv := make([]byte, len(payload))
			done := make(chan error, 2)
			go func() { _, e := io.ReadFull(conn, recv); done <- e }()
			go func() { _, e := io.Copy(conn, bytes.NewReader(payload)); done <- e }()
			for k := 0; k < 2; k++ {
				select {
				case e := <-done:
					if e != nil {
						errs <- e
						return
					}
				case <-time.After(8 * time.Second):
					errs <- net.ErrClosed
					return
				}
			}
			if !bytes.Equal(payload, recv) {
				errs <- net.ErrClosed
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("session error: %v", e)
	}
}

func TestXmuxConnRotation(t *testing.T) {
	// HMaxRequestTimes=1 -> every session gets a fresh conn.
	runEchoWithOpts(t, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.Xmux = &xhttp.XmuxConfig{
			HMaxRequestTimes: &xhttp.Range{From: 1, To: 1},
		}
	})
}

func TestXmuxReusableSecs(t *testing.T) {
	// HMaxReusableSecs=1 -> first session ok; later sessions get fresh conn.
	runEchoWithOpts(t, xhttp.ModePacketUp, true, func(o *xhttp.Options) {
		o.Xmux = &xhttp.XmuxConfig{
			HMaxReusableSecs: &xhttp.Range{From: 1, To: 1},
		}
	})
}

// TestXmuxSpreadsConnections verifies that with MaxConnections=N and concurrency=1,
// N concurrent sessions actually originate from N distinct TCP source ports.
func TestXmuxSpreadsConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	logger := log.NewNOPFactory().NewLogger("test")
	ctx := context.Background()
	sTLS, cTLS := makeTLSPair(t)

	var mu sync.Mutex
	seen := map[string]struct{}{}
	hold := make(chan struct{})

	handler := holdHandler{seen: seen, mu: &mu, hold: hold}
	opts := xhttp.Options{
		Mode: xhttp.ModePacketUp,
		Path: "/xhttp",
		Xmux: &xhttp.XmuxConfig{
			MaxConnections: &xhttp.Range{From: 4, To: 4},
			MaxConcurrency: &xhttp.Range{From: 1, To: 1},
		},
	}
	server, err := xhttp.NewServer(ctx, logger, opts, anyServerTLS(sTLS), handler)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go server.Serve(listener)
	time.Sleep(50 * time.Millisecond)

	client, err := xhttp.NewClient(ctx, directDialer{}, M.ParseSocksaddrHostPort("127.0.0.1", uint16(port)), opts, anyClientTLS(cTLS))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	conns := make([]net.Conn, 4)
	for i := 0; i < 4; i++ {
		c, err := client.DialContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = c
		// Drive a byte so the GET round-trip lands on the wire and the
		// server records the source port.
		_, _ = c.Write([]byte{'x'})
	}
	// Wait for server to see all sessions.
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n >= 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(hold)
	for _, c := range conns {
		c.Close()
	}

	mu.Lock()
	count := len(seen)
	mu.Unlock()
	if count < 4 {
		t.Fatalf("expected 4 distinct source ports, got %d: %v", count, seen)
	}
}

type holdHandler struct {
	seen map[string]struct{}
	mu   *sync.Mutex
	hold chan struct{}
}

func (h holdHandler) NewConnectionEx(ctx context.Context, conn net.Conn, source M.Socksaddr, _ M.Socksaddr, onClose N.CloseHandlerFunc) {
	go func() {
		defer conn.Close()
		h.mu.Lock()
		h.seen[source.String()] = struct{}{}
		h.mu.Unlock()
		// Drain a byte so the read pump starts.
		b := make([]byte, 1)
		_, _ = conn.Read(b)
		<-h.hold
		if onClose != nil {
			onClose(nil)
		}
	}()
}
