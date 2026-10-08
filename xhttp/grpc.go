package xhttp

import (
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"

	"google.golang.org/protobuf/encoding/protowire"
)

const (
	grpcSessionHeader  = "X-Xhttp-Session"
	maxGRPCMessageSize = 4 << 20
	grpcWriteChunkSize = 64 << 10
)

func grpcPath(basePath string) string {
	service := strings.Trim(basePath, "/")
	if service == "" {
		service = "xhttp"
	}
	return "/" + service + "/Tun"
}

func isGRPCContentType(value string) bool {
	return value == "application/grpc" || strings.HasPrefix(value, "application/grpc+")
}

// grpcReader decodes the same message schema as gRPC-lite:
// message Hunk { bytes data = 1; }. HTTP DATA frame boundaries are irrelevant.
type grpcReader struct {
	body   io.ReadCloser
	mu     sync.Mutex
	frame  []byte
	cache  []byte
	err    error
	closed atomic.Bool
}

func newGRPCReader(body io.ReadCloser) *grpcReader {
	return &grpcReader{body: body}
}

func (r *grpcReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed.Load() {
		return 0, net.ErrClosed
	}
	if r.err != nil {
		return 0, r.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.cache) == 0 {
		r.err = r.readMessage()
		if r.err != nil {
			return 0, r.err
		}
	}
	n := copy(p, r.cache)
	r.cache = r.cache[n:]
	return n, nil
}

func (r *grpcReader) readMessage() error {
	var header [5]byte
	if _, err := io.ReadFull(r.body, header[:]); err != nil {
		return err
	}
	if header[0] != 0 {
		return E.New("xhttp: compressed gRPC messages are unsupported")
	}
	size := binary.BigEndian.Uint32(header[1:])
	if size > maxGRPCMessageSize {
		return E.New("xhttp: gRPC message exceeds maximum size")
	}
	if cap(r.frame) < int(size) {
		r.frame = make([]byte, int(size))
	} else {
		r.frame = r.frame[:int(size)]
	}
	if _, err := io.ReadFull(r.body, r.frame); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	message := r.frame
	for len(message) != 0 {
		number, wireType, n := protowire.ConsumeTag(message)
		if n < 0 {
			return E.Cause(protowire.ParseError(n), "xhttp: invalid gRPC protobuf tag")
		}
		message = message[n:]
		if number == 1 && wireType == protowire.BytesType {
			r.cache, n = protowire.ConsumeBytes(message)
		} else {
			n = protowire.ConsumeFieldValue(number, wireType, message)
		}
		if n < 0 {
			return E.Cause(protowire.ParseError(n), "xhttp: invalid gRPC protobuf field")
		}
		message = message[n:]
	}
	return nil
}

func (r *grpcReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	err := r.body.Close()
	r.mu.Lock()
	r.frame, r.cache = nil, nil
	r.mu.Unlock()
	return err
}

// finish runs only after the response writer has been closed. Empty protobuf
// messages are valid heartbeats; malformed input must not end with status OK.
func (r *grpcReader) finish(w http.ResponseWriter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	status := "0"
	if r.err != nil && r.err != io.EOF {
		status = "13"
	}
	w.Header().Set("Grpc-Status", status)
}

type grpcWriter struct {
	body   io.WriteCloser
	mu     sync.Mutex
	err    error
	closed atomic.Bool
}

func newGRPCWriter(body io.WriteCloser) *grpcWriter {
	return &grpcWriter{body: body}
}

func (w *grpcWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed.Load() {
		return 0, net.ErrClosed
	}
	if w.err != nil {
		return 0, w.err
	}
	var written int
	for len(p) > 0 {
		size := min(len(p), grpcWriteChunkSize)
		var header [5 + 1 + binary.MaxVarintLen64]byte
		header[5] = 0x0a
		varLen := binary.PutUvarint(header[6:], uint64(size))
		binary.BigEndian.PutUint32(header[1:5], uint32(1+varLen+size))
		frame := buf.NewSize(6 + varLen + size)
		_, _ = frame.Write(header[:6+varLen])
		_, _ = frame.Write(p[:size])
		n, err := w.body.Write(frame.Bytes())
		if err == nil && n != frame.Len() {
			err = io.ErrShortWrite
		}
		frame.Release()
		if err != nil {
			w.err = err
			return written, err
		}
		written += size
		p = p[size:]
	}
	return written, nil
}

func (w *grpcWriter) Close() error {
	if w.closed.Swap(true) {
		return nil
	}
	// Closing the pipe must interrupt an in-flight Write without taking mu.
	return w.body.Close()
}

func startGRPCResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Add("Trailer", "Grpc-Status")
}

func grpcResponseReader(resp *http.Response) (io.ReadCloser, error) {
	if !isGRPCContentType(resp.Header.Get("Content-Type")) {
		return nil, E.New("xhttp: unexpected gRPC content type: ", resp.Header.Get("Content-Type"))
	}
	if encoding := resp.Header.Get("Grpc-Encoding"); encoding != "" && encoding != "identity" {
		return nil, E.New("xhttp: unsupported gRPC encoding: ", encoding)
	}
	if status := resp.Header.Get("Grpc-Status"); status != "" && status != "0" {
		return nil, E.New("xhttp: gRPC status ", status)
	}
	return newGRPCReader(&grpcResponseBody{Response: resp}), nil
}

// gRPC carries RPC failures in trailers even when the HTTP status is 200.
type grpcResponseBody struct{ *http.Response }

func (b *grpcResponseBody) Read(p []byte) (int, error) {
	n, err := b.Body.Read(p)
	if err == io.EOF {
		status := b.Trailer.Get("Grpc-Status")
		if status == "" {
			status = b.Header.Get("Grpc-Status") // trailers-only response
		}
		if status == "" {
			err = E.New("xhttp: missing gRPC status")
		} else if status != "0" {
			err = E.New("xhttp: gRPC status ", status)
		}
	}
	return n, err
}

func (b *grpcResponseBody) Close() error { return b.Body.Close() }
