package xhttp

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type streamRoundTripper func(*http.Request) (*http.Response, error)

func (f streamRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func streamTestClient(mode string, framed bool, transport http.RoundTripper) (*Client, *xmuxClient) {
	options := Options{Mode: mode, Path: "/xhttp", GRPCFraming: framed}
	ts := &transportSet{requestURL: url.URL{Scheme: "https", Host: "localhost", Path: "/xhttp"}, httpVersion: "2"}
	c := &Client{up: ts, down: ts, method: http.MethodPost, opts: options, codec: newCodec(options)}
	xc := &xmuxClient{conn: &xmuxConn{transport: transport}}
	xc.openUsage.Store(1)
	return c, xc
}

func TestStreamUpUploadEOFDoesNotDiscardDownload(t *testing.T) {
	for _, framed := range []bool{false, true} {
		t.Run(map[bool]string{false: "raw", true: "grpc"}[framed], func(t *testing.T) {
			c, xc := streamTestClient(ModeStreamUp, framed, streamRoundTripper(func(r *http.Request) (*http.Response, error) {
				resp := &http.Response{StatusCode: 200, Header: make(http.Header), Trailer: make(http.Header)}
				if r.Method == http.MethodGet {
					resp.Body = io.NopCloser(strings.NewReader("final download bytes"))
				} else {
					resp.Body = http.NoBody
					resp.Header.Set("Content-Type", "application/grpc")
					resp.Trailer.Set("Grpc-Status", "0")
				}
				return resp, nil
			}))
			conn, err := c.dialStreamUp(context.Background(), "session", xc, xc)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			// The fake peer has stopped reading the upload. Write must unblock
			// at upload EOF, while the independent download remains readable.
			writeDone := make(chan error, 1)
			go func() { _, err := conn.Write([]byte("late upload")); writeDone <- err }()
			select {
			case err := <-writeDone:
				if err == nil {
					t.Fatal("write succeeded after upload ended")
				}
			case <-time.After(time.Second):
				t.Fatal("upload EOF did not release Write")
			}
			body, err := io.ReadAll(conn)
			if err != nil || string(body) != "final download bytes" {
				t.Fatalf("download = %q, %v", body, err)
			}
		})
	}
}

type countedStreamBody struct {
	io.Reader
	closes atomic.Int32
	closed chan struct{}
}

func (b *countedStreamBody) Close() error {
	if b.closes.Add(1) == 1 && b.closed != nil {
		close(b.closed)
	}
	return nil
}

func TestStreamOneCloseBeforeResponse(t *testing.T) {
	for _, framed := range []bool{false, true} {
		t.Run(map[bool]string{false: "raw", true: "grpc"}[framed], func(t *testing.T) {
			started := make(chan *http.Request, 1)
			release := make(chan struct{})
			body := &countedStreamBody{Reader: bytes.NewReader(nil), closed: make(chan struct{})}
			c, xc := streamTestClient(ModeStreamOne, framed, streamRoundTripper(func(r *http.Request) (*http.Response, error) {
				started <- r
				<-release // simulate response headers racing with cancellation
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/grpc"}}, Body: body}, nil
			}))
			conn, err := c.dialStreamOne(context.Background(), "", xc, xc)
			if err != nil {
				t.Fatal(err)
			}
			req := <-started
			writeDone := make(chan error, 1)
			readDone := make(chan error, 1)
			go func() { _, err := conn.Write([]byte("blocked")); writeDone <- err }()
			go func() { _, err := conn.Read(make([]byte, 1)); readDone <- err }()
			_ = conn.Close()
			_ = conn.Close()
			close(release)
			for _, ch := range []<-chan error{writeDone, readDone} {
				select {
				case err := <-ch:
					if err == nil {
						t.Fatal("I/O succeeded after Close")
					}
				case <-time.After(time.Second):
					t.Fatal("Close did not interrupt I/O")
				}
			}
			select {
			case <-req.Context().Done():
			default:
				t.Fatal("Close did not cancel the HTTP request")
			}
			select {
			case <-body.closed:
			case <-time.After(time.Second):
				t.Fatal("late response body leaked")
			}
			if body.closes.Load() != 1 || xc.openUsage.Load() != 0 {
				t.Fatalf("body closes = %d, open usage = %d", body.closes.Load(), xc.openUsage.Load())
			}
		})
	}
}

func TestWaitReadCloserConcurrentCloseAndSetup(t *testing.T) {
	for range 100 {
		body := &countedStreamBody{Reader: bytes.NewReader(nil)}
		r := newWaitReadCloser()
		var wg sync.WaitGroup
		wg.Go(func() { r.set(body) })
		wg.Go(func() { _ = r.Close() })
		wg.Go(func() { _ = r.Close() })
		wg.Wait()
		if body.closes.Load() != 1 {
			t.Fatalf("response body closed %d times", body.closes.Load())
		}
	}
}
