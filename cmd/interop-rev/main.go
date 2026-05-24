// Reverse interop: sing-xhttp server, Xray-core splithttp client.
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

	"github.com/justinwoo280/sing-xhttp/xhttp"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type echoHandler struct{}

func (echoHandler) NewConnectionEx(ctx context.Context, conn net.Conn, _ M.Socksaddr, _ M.Socksaddr, onClose N.CloseHandlerFunc) {
	fmt.Fprintln(os.Stderr, "[sing-xhttp-server] new conn")
	go func() {
		defer conn.Close()
		io.Copy(conn, conn)
		if onClose != nil { onClose(nil) }
	}()
}

func main() {
	mode := flag.String("mode", "packet-up", "packet-up|stream-up (stream-up needs TLS)")
	size := flag.Int("size", 64*1024, "bytes to echo")
	sessionPlace := flag.String("session-placement", "", "path|query|header|cookie")
	seqPlace := flag.String("seq-placement", "", "path|query|header|cookie")
	xpadObfs := flag.Bool("x-padding-obfs", false, "enable XPaddingObfsMode")
	xpadPlace := flag.String("x-padding-placement", "", "query|header|cookie (when obfs)")
	xpadMethod := flag.String("x-padding-method", "", "repeat-x|tokenish (when obfs)")
	xpadKey := flag.String("x-padding-key", "", "cookie/query key (when obfs)")
	xpadHeader := flag.String("x-padding-header", "", "header name (when obfs)")
	flag.Parse()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	port := ln.Addr().(*net.TCPAddr).Port

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
	server, err := xhttp.NewServer(context.Background(), logger, opts, nil, echoHandler{})
	must(err)
	go server.Serve(ln)
	fmt.Fprintf(os.Stderr, "[sing-xhttp-server] mode=%s on 127.0.0.1:%d\n", *mode, port)
	time.Sleep(100 * time.Millisecond)

	// --- Xray splithttp client (raw transport.Dial) ---
	xrayCfg := &xsplithttp.Config{
		Path: "/xhttp",
		Mode: *mode,
		ScMaxEachPostBytes: &xsplithttp.RangeConfig{From: 8192, To: 8192},
		SessionPlacement: *sessionPlace,
		SeqPlacement:     *seqPlace,
		XPaddingObfsMode: *xpadObfs,
		XPaddingPlacement: *xpadPlace,
		XPaddingMethod:    *xpadMethod,
		XPaddingKey:       *xpadKey,
		XPaddingHeader:    *xpadHeader,
	}
	memCfg := &xinternet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: xrayCfg,
	}
	dest := xnet.TCPDestination(xnet.ParseAddress("127.0.0.1"), xnet.Port(port))
	conn, err := xsplithttp.Dial(context.Background(), dest, memCfg)
	must(err)
	defer conn.Close()

	payload := make([]byte, *size)
	rand.Read(payload)
	recv := make([]byte, len(payload))

	var wg sync.WaitGroup
	wg.Add(2)
	var readErr, writeErr error
	go func() { defer wg.Done(); _, readErr = io.ReadFull(conn, recv) }()
	go func() { defer wg.Done(); _, writeErr = io.Copy(conn, bytes.NewReader(payload)) }()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		fmt.Println("TIMEOUT"); os.Exit(2)
	}
	if readErr != nil { fmt.Println("read err:", readErr); os.Exit(2) }
	if writeErr != nil { fmt.Println("write err:", writeErr); os.Exit(2) }
	if !bytes.Equal(payload, recv) {
		fmt.Println("DATA MISMATCH"); os.Exit(2)
	}
	fmt.Printf("OK: %s mode, %d bytes round-tripped between xray client and sing-xhttp server\n", *mode, *size)
}

func must(err error) {
	if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
