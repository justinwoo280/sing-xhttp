package xhttp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func grpcTestFrame(message []byte) []byte {
	header := make([]byte, 5, 5+len(message))
	binary.BigEndian.PutUint32(header[1:], uint32(len(message)))
	return append(header, message...)
}

func TestGRPCReaderMessageBoundaries(t *testing.T) {
	var wire []byte
	// Unknown varint/group fields and repeated bytes fields follow protobuf
	// semantics: the final occurrence wins, and an empty message is skipped.
	for _, message := range [][]byte{
		nil,
		{0x10, 0x7f},
		{0x0a, 3, 'o', 'l', 'd', 0x13, 0x18, 1, 0x14, 0x0a, 4, 'p', 'i', 'n', 'g'},
		{0x0a, 0},
		{0x0a, 4, 'p', 'o', 'n', 'g'},
	} {
		wire = append(wire, grpcTestFrame(message)...)
	}
	r := newGRPCReader(io.NopCloser(iotest.OneByteReader(bytes.NewReader(wire))))
	defer r.Close()
	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Fatalf("empty read = %d, %v", n, err)
	}
	data, err := io.ReadAll(r)
	if err != nil || string(data) != "pingpong" {
		t.Fatalf("read = %q, %v", data, err)
	}
}

func TestGRPCReaderRejectsMalformedMessages(t *testing.T) {
	for name, wire := range map[string][]byte{
		"compression":        {1, 0, 0, 0, 0},
		"reserved flag":      {2, 0, 0, 0, 0},
		"oversize":           {0, 0xff, 0xff, 0xff, 0xff},
		"truncated header":   {0, 0, 0},
		"missing message":    {0, 0, 0, 0, 1},
		"truncated message":  {0, 0, 0, 0, 3, 0x0a, 1},
		"invalid field tag":  grpcTestFrame([]byte{0}),
		"truncated protobuf": grpcTestFrame([]byte{0x0a, 2, 'x'}),
		"invalid wire type":  grpcTestFrame([]byte{0x0f}),
	} {
		t.Run(name, func(t *testing.T) {
			r := newGRPCReader(io.NopCloser(bytes.NewReader(wire)))
			defer r.Close()
			var p [16]byte
			_, err := r.Read(p[:])
			if err == nil || err == io.EOF {
				t.Fatalf("malformed message returned %v", err)
			}
			if _, again := r.Read(p[:]); again != err {
				t.Fatalf("error was not sticky: %v -> %v", err, again)
			}
		})
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func TestGRPCWriterConcurrentMessages(t *testing.T) {
	var wire bytes.Buffer
	w := newGRPCWriter(nopWriteCloser{&wire})
	const count = 16
	var wg sync.WaitGroup
	for i := range count {
		wg.Go(func() {
			payload := bytes.Repeat([]byte{byte(i)}, 1000)
			if n, err := w.Write(payload); n != len(payload) || err != nil {
				t.Errorf("Write = %d, %v", n, err)
			}
		})
	}
	wg.Wait()
	seen := make(map[byte]bool)
	for wire.Len() > 0 {
		var header [5]byte
		if _, err := io.ReadFull(&wire, header[:]); err != nil {
			t.Fatal(err)
		}
		message := new(wrapperspb.BytesValue)
		size := binary.BigEndian.Uint32(header[1:])
		if header[0] != 0 || int(size) > wire.Len() {
			t.Fatal("invalid gRPC header")
		}
		if err := proto.Unmarshal(wire.Next(int(size)), message); err != nil {
			t.Fatal(err)
		}
		if len(message.Value) != 1000 || !bytes.Equal(message.Value, bytes.Repeat(message.Value[:1], 1000)) {
			t.Fatal("concurrent writes interleaved")
		}
		if seen[message.Value[0]] {
			t.Fatal("duplicate message")
		}
		seen[message.Value[0]] = true
	}
	if len(seen) != count {
		t.Fatalf("received %d messages", len(seen))
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestGRPCWriterShortWriteIsTerminal(t *testing.T) {
	w := newGRPCWriter(nopWriteCloser{shortWriter{}})
	for range 2 {
		if n, err := w.Write([]byte("hello")); n != 0 || err != io.ErrShortWrite {
			t.Fatalf("Write = %d, %v", n, err)
		}
	}
}

type startedWriteCloser struct {
	io.WriteCloser
	started chan struct{}
}

func (w startedWriteCloser) Write(p []byte) (int, error) {
	close(w.started)
	return w.WriteCloser.Write(p)
}

func TestGRPCCloseInterruptsIO(t *testing.T) {
	pr, pw := io.Pipe()
	started := make(chan struct{})
	w := newGRPCWriter(startedWriteCloser{pw, started})
	done := make(chan error, 1)
	go func() { _, err := w.Write([]byte("blocked")); done <- err }()
	<-started
	_ = w.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked Write succeeded after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt Write")
	}
	_ = pr.Close()
	pr, pw = io.Pipe()
	r := newGRPCReader(pr)
	go func() { _, err := r.Read(make([]byte, 1)); done <- err }()
	_ = r.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked Read succeeded after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt Read")
	}
	_ = pw.Close()
	if _, err := r.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read after Close = %v", err)
	}
}

func TestGRPCResponseStatus(t *testing.T) {
	for _, tt := range []struct {
		name, contentType, encoding, headerStatus, trailerStatus, wantError string
	}{
		{name: "success", contentType: "application/grpc", trailerStatus: "0"},
		{name: "trailers only", contentType: "application/grpc+proto", headerStatus: "0"},
		{name: "SSE", contentType: "text/event-stream", wantError: "content type"},
		{name: "grpc-web", contentType: "application/grpc-web", wantError: "content type"},
		{name: "compressed", contentType: "application/grpc", encoding: "gzip", wantError: "encoding"},
		{name: "missing status", contentType: "application/grpc", wantError: "missing gRPC status"},
		{name: "failure", contentType: "application/grpc", trailerStatus: "14", wantError: "status 14"},
		{name: "trailers only failure", contentType: "application/grpc", headerStatus: "7", wantError: "status 7"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				Header: make(http.Header), Trailer: make(http.Header),
				Body: io.NopCloser(bytes.NewReader(nil)),
			}
			resp.Header.Set("Content-Type", tt.contentType)
			resp.Header.Set("Grpc-Encoding", tt.encoding)
			resp.Header.Set("Grpc-Status", tt.headerStatus)
			resp.Trailer.Set("Grpc-Status", tt.trailerStatus)
			r, err := grpcResponseReader(resp)
			if err == nil {
				_, err = io.ReadAll(r)
				_ = r.Close()
			}
			if tt.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error = %v, want %q", err, tt.wantError)
			}
		})
	}
}
