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
