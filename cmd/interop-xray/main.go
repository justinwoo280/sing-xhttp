// Interop test: Xray-core splithttp listener as server, sing-xhttp client.
//
// The Xray side is wired at the raw transport layer (no VLESS), so what we
// verify is purely wire-level XHTTP compatibility: padding, session/seq
// path placement, SSE/gRPC headers, packet-up reassembly, etc.
//
// Run: go run ./cmd/interop-xray
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	xinternet "github.com/xtls/xray-core/transport/internet"
	xsplithttp "github.com/xtls/xray-core/transport/internet/splithttp"
	xstat "github.com/xtls/xray-core/transport/internet/stat"

	"github.com/justinwoo280/sing-xhttp/xhttp"
	aTLS "github.com/sagernet/sing/common/tls"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

func main() {
	mode := flag.String("mode", "packet-up", "packet-up|stream-up")
	size := flag.Int("size", 64*1024, "bytes to echo")
	sessionPlace := flag.String("session-placement", "", "path|query|header|cookie (xray + sing-xhttp must match)")
	seqPlace := flag.String("seq-placement", "", "path|query|header|cookie")
	xpadObfs := flag.Bool("x-padding-obfs", false, "enable XPaddingObfsMode")
	xpadPlace := flag.String("x-padding-placement", "", "query|header|cookie (when obfs)")
	xpadMethod := flag.String("x-padding-method", "", "repeat-x|tokenish (when obfs)")
	xpadKey := flag.String("x-padding-key", "", "cookie/query key (when obfs)")
	xpadHeader := flag.String("x-padding-header", "", "header name (when obfs)")
	flag.Parse()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	// --- Server: Xray splithttp.ListenXH ---
	xrayCfg := &xsplithttp.Config{
		Path: "/xhttp",
		Mode: *mode,
		SessionPlacement: *sessionPlace,
		SeqPlacement:     *seqPlace,
		XPaddingObfsMode: *xpadObfs,
		XPaddingPlacement: *xpadPlace,
		XPaddingMethod:    *xpadMethod,
		XPaddingKey:       *xpadKey,
		XPaddingHeader:    *xpadHeader,
	}
	memCfg := &xinternet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: xrayCfg,
	}
	addr, _ := xnet.ParseAddress("127.0.0.1"), xnet.ParseAddress("127.0.0.1")
	_ = addr

	addConn := func(c xstat.Connection) {
		fmt.Fprintln(os.Stderr, "[xray-server] new conn")
		go func() {
			defer c.Close()
			io.Copy(c, c)
		}()
	}
	xln, err := xsplithttp.ListenXH(context.Background(), xnet.ParseAddress("127.0.0.1"), xnet.Port(port), memCfg, addConn)
	must(err)
	defer xln.Close()
	fmt.Fprintf(os.Stderr, "[xray-server] mode=%s listening on 127.0.0.1:%d\n", *mode, port)
	time.Sleep(100 * time.Millisecond)

	// --- Client: sing-xhttp ---
	logger := logger.NOP()
	opts := xhttp.Options{
		Mode: *mode,
		Path: "/xhttp",
		ScMaxEachPostBytes: &xhttp.Range{From: 8192, To: 8192},
		SessionPlacement: *sessionPlace,
		SeqPlacement:     *seqPlace,
		XPaddingObfsMode: *xpadObfs,
		XPaddingPlacement: *xpadPlace,
		XPaddingMethod:    *xpadMethod,
		XPaddingKey:       *xpadKey,
		XPaddingHeader:    *xpadHeader,
	}
	// stream-up requires TLS in our impl. The wire test below covers packet-up
	// (which works on plaintext H1/H2) against Xray's plaintext listener.
	var tlsCfg aTLS.Config = nil
	if *mode == "stream-up" {
		fmt.Fprintln(os.Stderr, "NOTE: stream-up against Xray needs TLS; sing-xhttp's H1 stream-up is unsupported by design.")
		fmt.Fprintln(os.Stderr, "      Falling back to packet-up for interop demo against Xray's plaintext listener.")
		opts.Mode = "packet-up"
	}

	client, err := xhttp.NewClient(context.Background(), directDialer{}, M.ParseSocksaddrHostPort("127.0.0.1", uint16(port)), opts, tlsCfg)
	must(err)

	conn, err := client.DialContext(context.Background())
	must(err)
	defer conn.Close()

	payload := make([]byte, *size)
	rand.Read(payload)
	recv := make([]byte, len(payload))

	var wg sync.WaitGroup
	wg.Add(2)
	var readErr, writeErr error
	go func() {
		defer wg.Done()
		_, readErr = io.ReadFull(conn, recv)
	}()
	go func() {
		defer wg.Done()
		_, writeErr = io.Copy(conn, bytes.NewReader(payload))
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		fmt.Println("TIMEOUT")
		logger.Error("timeout")
		os.Exit(2)
	}
	if readErr != nil { fmt.Println("read err:", readErr); os.Exit(2) }
	if writeErr != nil { fmt.Println("write err:", writeErr); os.Exit(2) }
	if !bytes.Equal(payload, recv) {
		fmt.Println("DATA MISMATCH")
		os.Exit(2)
	}
	fmt.Printf("OK: %s mode, %d bytes round-tripped between sing-xhttp client and xray splithttp server\n", opts.Mode, *size)
}

type directDialer struct{}

func (directDialer) DialContext(ctx context.Context, network string, dest M.Socksaddr) (net.Conn, error) {
	return net.Dial(network, dest.String())
}
func (directDialer) ListenPacket(ctx context.Context, dest M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket("udp", "")
}

func must(err error) {
	if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
