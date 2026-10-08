package xhttp_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinwoo280/sing-xhttp/xhttp"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"

	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func startH2Handler(t *testing.T, handler http.Handler, tlsConfig *serverTLS) *httptest.Server {
	t.Helper()
	s := httptest.NewUnstartedServer(handler)
	s.EnableHTTP2 = true
	s.TLS = tlsConfig.cfg.Clone()
	s.StartTLS()
	t.Cleanup(s.Close)
	return s
}

func grpcClient(t *testing.T, server *httptest.Server, tlsConfig *clientTLS) *grpc.ClientConn {
	t.Helper()
	c, err := grpc.NewClient(server.Listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig.cfg.Clone())))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// This proxy terminates both gRPC hops and unmarshals/remarshals protobuf
// messages using grpc-go. It deliberately sends no response headers until the
// first response message. A byte-copying HTTP proxy cannot prove RPC interop.
func startGRPCProxy(t *testing.T, origin *httptest.Server, sTLS *serverTLS, cTLS *clientTLS) (*httptest.Server, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	backend := grpcClient(t, origin, cTLS)
	uploads, downloads := new(atomic.Int32), new(atomic.Int32)
	proxy := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, incoming grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(incoming)
		md, _ := metadata.FromIncomingContext(incoming.Context())
		outgoing := metadata.MD{}
		for _, key := range []string{"referer", "x-xhttp-session", "x-padding", "cookie"} {
			if values := md.Get(key); len(values) != 0 {
				outgoing.Set(key, values...)
			}
		}
		ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(incoming.Context(), outgoing))
		defer cancel()
		stream, err := backend.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, method)
		if err != nil {
			return err
		}
		upErr := make(chan error, 1)
		go func() {
			for {
				message := new(wrapperspb.BytesValue)
				err := incoming.RecvMsg(message)
				if err == io.EOF {
					upErr <- stream.CloseSend()
					return
				}
				if err == nil {
					uploads.Add(1)
					err = stream.SendMsg(message)
				}
				if err != nil {
					upErr <- err
					cancel()
					return
				}
			}
		}()
		for {
			message := new(wrapperspb.BytesValue)
			if err := stream.RecvMsg(message); err != nil {
				incoming.SetTrailer(stream.Trailer())
				if err == io.EOF {
					return nil
				}
				select {
				case uploadErr := <-upErr:
					if uploadErr != nil {
						return uploadErr
					}
				default:
				}
				return err
			}
			downloads.Add(1)
			if err := incoming.SendMsg(message); err != nil {
				return err
			}
		}
	}))
	t.Cleanup(proxy.Stop)
	originURL, _ := url.Parse(origin.URL)
	httpProxy := httputil.NewSingleHostReverseProxy(originURL)
	transport := &http2.Transport{TLSClientConfig: cTLS.cfg.Clone()}
	httpProxy.Transport = transport
	httpProxy.FlushInterval = -1
	t.Cleanup(transport.CloseIdleConnections)
	front := startH2Handler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			httpProxy.ServeHTTP(w, r)
		} else {
			proxy.ServeHTTP(w, r)
		}
	}), sTLS)
	return front, uploads, downloads
}

func TestGRPCFramingThroughMessageProxy(t *testing.T) {
	for _, mode := range []string{xhttp.ModeStreamUp, xhttp.ModeStreamOne} {
		t.Run(mode, func(t *testing.T) {
			sTLS, cTLS := makeTLSPair(t)
			opts := xhttp.Options{
				Mode: mode, Path: "/xhttp", GRPCFraming: true,
				ScStreamUpServerSecs: &xhttp.Range{From: 1, To: 1},
			}
			server, err := xhttp.NewServer(context.Background(), logger.NOP(), opts, nil, echoHandler{})
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			origin := startH2Handler(t, server, sTLS)
			front, uploads, downloads := startGRPCProxy(t, origin, sTLS, cTLS)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client, err := xhttp.NewClient(ctx, directDialer{}, M.SocksaddrFromNet(front.Listener.Addr()), opts, cTLS)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			conn, err := client.DialContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			// One application Write larger than grpc-go's default 4 MiB receive
			// limit must be split into bounded messages on the wire.
			payload := bytes.Repeat([]byte("framed-xhttp\x00\xff"), 400000)
			writeDone := make(chan error, 1)
			go func() {
				_, err := conn.Write(payload)
				writeDone <- err
			}()
			reply := make([]byte, len(payload))
			if _, err = io.ReadFull(conn, reply); err != nil {
				t.Fatal(err)
			}
			if err = <-writeDone; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(reply, payload) {
				t.Fatal("data changed across gRPC proxy")
			}
			if uploads.Load() < 2 {
				t.Fatal("proxy did not decode multiple upload messages")
			}
			if mode == xhttp.ModeStreamUp {
				// The upload response's heartbeat must also be valid protobuf.
				timer := time.NewTimer(3 * time.Second)
				defer timer.Stop()
				tick := time.NewTicker(10 * time.Millisecond)
				defer tick.Stop()
				for downloads.Load() == 0 {
					select {
					case <-tick.C:
					case <-timer.C:
						t.Fatal("no valid gRPC upload-response heartbeat")
					}
				}
			} else if downloads.Load() < 2 {
				t.Fatal("proxy did not decode multiple download messages")
			}
		})
	}
}

// A stock grpc-go caller checks response content type, protobuf decoding,
// half-close behavior and the final grpc-status trailer independently.
func TestGRPCFramingWithStandardClient(t *testing.T) {
	sTLS, cTLS := makeTLSPair(t)
	server, err := xhttp.NewServer(context.Background(), logger.NOP(), xhttp.Options{
		Mode: xhttp.ModeStreamOne, Path: "/xhttp", GRPCFraming: true,
		XPaddingBytes: &xhttp.Range{From: 1, To: 1},
	}, nil, echoHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	origin := startH2Handler(t, server, sTLS)
	client := grpcClient(t, origin, cTLS)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("referer", "https://localhost/xhttp/?x_padding=X"))
	stream, err := client.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/xhttp/Tun")
	if err != nil {
		t.Fatal(err)
	}
	// Empty protobuf messages must not eat bytes from the next gRPC message.
	for _, value := range [][]byte{nil, []byte("ping"), nil, []byte("pong")} {
		if err := stream.SendMsg(wrapperspb.Bytes(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var received []byte
	for {
		message := new(wrapperspb.BytesValue)
		err := stream.RecvMsg(message)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("gRPC response/trailer: %v", err)
		}
		received = append(received, message.Value...)
	}
	if string(received) != "pingpong" {
		t.Fatalf("reply = %q", received)
	}
}

func TestGRPCFramingHTTP3(t *testing.T) {
	for _, mode := range []string{xhttp.ModeStreamUp, xhttp.ModeStreamOne} {
		t.Run(mode, func(t *testing.T) {
			sTLS, cTLS := makeTLSPair(t)
			sTLS.SetNextProtos([]string{"h3"})
			cTLS.SetNextProtos([]string{"h3"})
			opts := xhttp.Options{Mode: mode, Path: "/xhttp", GRPCFraming: true}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server, err := xhttp.NewServer(ctx, logger.NOP(), opts, sTLS, echoHandler{})
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			listener, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			go server.ServePacket(listener)
			client, err := xhttp.NewClient(ctx, directDialer{}, M.SocksaddrFromNet(listener.LocalAddr()), opts, cTLS)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			conn, err := client.DialContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			payload := bytes.Repeat([]byte("QUIC framed streaming\n"), 10000)
			writeDone := make(chan error, 1)
			go func() { _, err := conn.Write(payload); writeDone <- err }()
			reply := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, reply); err != nil {
				t.Fatal(err)
			}
			if err := <-writeDone; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(reply, payload) {
				t.Fatal("HTTP/3 data mismatch")
			}
		})
	}
}
