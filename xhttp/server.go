package xhttp

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	aTLS "github.com/sagernet/sing/common/tls"
	sHttp "github.com/sagernet/sing/protocol/http"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

var _ ServerTransport = (*Server)(nil)

type Server struct {
	ctx        context.Context
	logger     logger.ContextLogger
	tlsConfig  aTLS.ServerConfig
	handler    ServerHandler
	httpServer *http.Server
	h2Server   *http2.Server
	h2cHandler http.Handler

	host       string
	path       string
	method     string // "" means accept any uplink method (default POST)
	headers    http.Header
	opts       Options

	codec              *codec
	maxEachPostBytes   Range
	maxBufferedPosts   int
	streamUpServerSecs Range

	sessionsMu sync.Mutex
	sessions   map[string]*httpSession
}

type httpSession struct {
	queue          *uploadQueue
	connected      chan struct{} // closed once GET arrives
	connectedOnce  sync.Once
	uplinkDecided  chan struct{} // closed when uplink mode is known
	uplinkOnce     sync.Once
	streamUpReader io.ReadCloser // set if stream-up; nil if packet-up
}

func (s *httpSession) decideStreamUp(body io.ReadCloser) bool {
	done := false
	s.uplinkOnce.Do(func() {
		s.streamUpReader = body
		close(s.uplinkDecided)
		done = true
	})
	return done
}

func (s *httpSession) decidePacketUp() {
	s.uplinkOnce.Do(func() {
		close(s.uplinkDecided)
	})
}

func (s *httpSession) markConnected() {
	s.connectedOnce.Do(func() { close(s.connected) })
}

// tcpReadHeaderTimeout matches sing-box constant.TCPTimeout. Inlined so this
// library has no sing-box dependency.
const tcpReadHeaderTimeout = 15 * time.Second

func NewServer(ctx context.Context, logger logger.ContextLogger, options Options, tlsConfig aTLS.ServerConfig, handler ServerHandler) (*Server, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	if options.Mode == "" {
		options.Mode = ModePacketUp
	}
	if !strings.HasPrefix(options.Path, "/") {
		options.Path = "/" + options.Path
	}
	s := &Server{
		ctx:                ctx,
		logger:             logger,
		tlsConfig:          tlsConfig,
		handler:            handler,
		h2Server:           &http2.Server{},
		host:               options.Host,
		path:               options.Path,
		method:             options.Method,
		headers:            options.Headers.Build(),
		opts:               options,
		codec:              newCodec(options),
		maxEachPostBytes:   options.ScMaxEachPostBytes.orDefault(1_000_000, 1_000_000),
		maxBufferedPosts:   intOr(options.ScMaxBufferedPosts, 30),
		streamUpServerSecs: options.ScStreamUpServerSecs.orDefault(20, 80),
		sessions:           make(map[string]*httpSession),
	}
	s.httpServer = &http.Server{
		Handler:           s,
		ReadHeaderTimeout: tcpReadHeaderTimeout,
		MaxHeaderBytes:    http.DefaultMaxHeaderBytes,
		BaseContext: func(net.Listener) context.Context { return ctx },
		ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
			return contextWithNewConnID(ctx)
		},
	}
	s.h2cHandler = h2c.NewHandler(s, s.h2Server)
	return s, nil
}

// contextWithNewConnID attaches a fresh per-connection ID. Replaces
// sing-box's log.ContextWithNewID so this library has no sing-box dep.
// Embedding apps that want to bridge their own trace IDs can wrap this
// context themselves.
func contextWithNewConnID(ctx context.Context) context.Context {
	return context.WithValue(ctx, connIDKey{}, randomSeed())
}

type connIDKey struct{}

func intOr(v int32, d int) int {
	if v <= 0 {
		return d
	}
	return int(v)
}

func (s *Server) Network() []string { return []string{N.NetworkTCP} }

func (s *Server) Serve(listener net.Listener) error {
	if s.tlsConfig != nil {
		if len(s.tlsConfig.NextProtos()) == 0 {
			s.tlsConfig.SetNextProtos([]string{http2.NextProtoTLS, "http/1.1"})
		} else if !common.Contains(s.tlsConfig.NextProtos(), http2.NextProtoTLS) {
			s.tlsConfig.SetNextProtos(append([]string{http2.NextProtoTLS}, s.tlsConfig.NextProtos()...))
		}
		listener = aTLS.NewListener(listener, s.tlsConfig)
		return s.httpServer.Serve(listener)
	}
	// h2c support for plaintext
	s.httpServer.Handler = s.h2cHandler
	return s.httpServer.Serve(listener)
}

func (s *Server) ServePacket(listener net.PacketConn) error { return os.ErrInvalid }

func (s *Server) Close() error {
	return common.Close(common.PtrOrNil(s.httpServer))
}

func (s *Server) upsertSession(sessionID string) *httpSession {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	if existing, ok := s.sessions[sessionID]; ok {
		return existing
	}
	sess := &httpSession{
		queue:         newUploadQueue(s.maxBufferedPosts),
		connected:     make(chan struct{}),
		uplinkDecided: make(chan struct{}),
	}
	s.sessions[sessionID] = sess
	// reap if GET never arrives within 30s
	go func() {
		select {
		case <-time.After(30 * time.Second):
			s.sessionsMu.Lock()
			if s.sessions[sessionID] == sess {
				delete(s.sessions, sessionID)
			}
			s.sessionsMu.Unlock()
			_ = sess.queue.Close()
		case <-sess.connected:
		}
	}()
	return sess
}

func (s *Server) deleteSession(sessionID string) {
	s.sessionsMu.Lock()
	delete(s.sessions, sessionID)
	s.sessionsMu.Unlock()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "PRI" && len(r.Header) == 0 && r.URL.Path == "*" && r.Proto == "HTTP/2.0" {
		s.h2cHandler.ServeHTTP(w, r)
		return
	}

	if s.host != "" && !strings.EqualFold(r.Host, s.host) {
		s.invalid(w, r, http.StatusNotFound, E.New("bad host: ", r.Host))
		return
	}
	if !strings.HasPrefix(r.URL.Path, s.codec.basePath) {
		s.invalid(w, r, http.StatusNotFound, E.New("bad path: ", r.URL.Path))
		return
	}

	// Apply common response headers / CORS.
	w.Header().Set("Cache-Control", "no-store")
	if !s.opts.NoSSEHeader {
		// only really useful on GET responses, but harmless to set early.
	}
	for k, vs := range s.headers {
		for _, v := range vs {
			w.Header().Set(k, v)
		}
	}
	s.codec.applyPaddingToResponseHeader(w)

	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.WriteHeader(http.StatusOK)
		return
	}

	// Validate x_padding to prevent probing.
	paddingValue := s.codec.extractPaddingFromRequest(r)
	if !s.codec.validatePadding(paddingValue) {
		s.invalid(w, r, http.StatusBadRequest, E.New("invalid x_padding"))
		return
	}

	sessionID, seqStr, ok := s.codec.extractMetaFromRequest(r)
	if !ok {
		s.invalid(w, r, http.StatusNotFound, E.New("path doesn't match base"))
		return
	}

	isUplink := r.Method != http.MethodGet || seqStr != ""
	if isUplink && sessionID == "" {
		s.invalid(w, r, http.StatusBadRequest, E.New("upload without sessionId"))
		return
	}

	switch {
	case isUplink && seqStr != "":
		s.handlePacketUpPost(w, r, sessionID, seqStr)
	case isUplink && seqStr == "":
		s.handleStreamUpPost(w, r, sessionID)
	default:
		s.handleDownloadGet(w, r, sessionID)
	}
}

func (s *Server) handlePacketUpPost(w http.ResponseWriter, r *http.Request, sessionID, seqStr string) {
	if s.opts.Mode != "" && s.opts.Mode != ModePacketUp {
		s.invalid(w, r, http.StatusBadRequest, E.New("packet-up not allowed"))
		return
	}
	max := int(s.maxEachPostBytes.To)
	if max <= 0 {
		max = 1_000_000
	}
	if r.ContentLength > int64(max) {
		s.invalid(w, r, http.StatusRequestEntityTooLarge, E.New("upload too large"))
		return
	}
	var body bytes.Buffer
	lim := io.LimitReader(r.Body, int64(max)+1)
	_, err := io.Copy(&body, lim)
	if err != nil {
		s.invalid(w, r, http.StatusBadRequest, E.Cause(err, "read body"))
		return
	}
	if body.Len() > max {
		s.invalid(w, r, http.StatusRequestEntityTooLarge, E.New("upload too large"))
		return
	}
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		s.invalid(w, r, http.StatusBadRequest, E.Cause(err, "bad seq"))
		return
	}
	sess := s.upsertSession(sessionID)
	sess.decidePacketUp()
	if err := sess.queue.Push(packet{payload: body.Bytes(), seq: seq}); err != nil {
		s.invalid(w, r, http.StatusConflict, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleStreamUpPost(w http.ResponseWriter, r *http.Request, sessionID string) {
	if s.opts.Mode != "" && s.opts.Mode != ModeStreamUp {
		s.invalid(w, r, http.StatusBadRequest, E.New("stream-up not allowed"))
		return
	}
	sess := s.upsertSession(sessionID)
	if !sess.decideStreamUp(r.Body) {
		s.invalid(w, r, http.StatusConflict, E.New("uplink already attached"))
		return
	}
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	// Optional reverse heartbeat to keep CDN from killing the long POST.
	done := make(chan struct{})
	go func() {
		if r.Header.Get("Referer") == "" || s.streamUpServerSecs.To == 0 {
			return
		}
		tk := time.NewTimer(time.Duration(rangeRand(s.streamUpServerSecs)) * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-done:
				return
			case <-tk.C:
				if _, err := w.Write(bytes.Repeat([]byte{'X'}, int(rangeRand(s.codec.xpadRange)))); err != nil {
					return
				}
				if fl, ok := w.(http.Flusher); ok {
					fl.Flush()
				}
				tk.Reset(time.Duration(rangeRand(s.streamUpServerSecs)) * time.Second)
			}
		}
	}()

	// Wait until the session's GET ends (or POST itself errors).
	<-r.Context().Done()
	close(done)
}

func (s *Server) handleDownloadGet(w http.ResponseWriter, r *http.Request, sessionID string) {
	var sess *httpSession
	if sessionID != "" {
		sess = s.upsertSession(sessionID)
		sess.markConnected()
		defer s.deleteSession(sessionID)
	}

	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Cache-Control", "no-store")
	if !s.opts.NoSSEHeader {
		w.Header().Set("Content-Type", "text/event-stream")
	}
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	// Build the reader side of the splitConn: either the reorder queue or the
	// stream-up POST body (if/when it arrives).
	var reader io.ReadCloser = io.NopCloser(io.Reader(nil))
	if sess != nil {
		reader = newLazyStreamReader(sess)
	}

	writer := &flushWriter{w: w, flusher: flusher}
	done := make(chan struct{})
	conn := &splitConn{
		reader: reader,
		writer: writer,
		local:  nil,
		remote: parseRemote(r),
		onClose: func() error {
			close(done)
			return nil
		},
	}

	source := sHttp.SourceAddress(r)
	s.handler.NewConnectionEx(r.Context(), conn, source, M.Socksaddr{}, nil)

	select {
	case <-r.Context().Done():
	case <-done:
	}
	if sess != nil {
		_ = sess.queue.Close()
	}
}

func (s *Server) invalid(w http.ResponseWriter, r *http.Request, code int, err error) {
	if code > 0 {
		w.WriteHeader(code)
	}
	if s.logger != nil {
		s.logger.ErrorContext(r.Context(), E.Cause(err, "xhttp from ", r.RemoteAddr))
	}
}

func parseRemote(r *http.Request) net.Addr {
	if a, err := net.ResolveTCPAddr("tcp", r.RemoteAddr); err == nil {
		return a
	}
	return &net.TCPAddr{}
}

// flushWriter flushes after every write so downlink bytes hit the wire ASAP.
type flushWriter struct {
	mu      sync.Mutex
	w       io.Writer
	flusher http.Flusher
	closed  bool
}

func (f *flushWriter) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, io.ErrClosedPipe
	}
	n, err := f.w.Write(b)
	if err == nil && f.flusher != nil {
		f.flusher.Flush()
	}
	return n, err
}

func (f *flushWriter) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}

// lazyStreamReader dispatches reads to either the uploadQueue (packet-up) or
// the stream-up POST body (stream-up), whichever is configured on the session.
type lazyStreamReader struct {
	sess *httpSession
	once sync.Once
	src  io.Reader
}

func newLazyStreamReader(sess *httpSession) *lazyStreamReader {
	return &lazyStreamReader{sess: sess}
}

func (l *lazyStreamReader) Read(b []byte) (int, error) {
	l.once.Do(func() {
		<-l.sess.uplinkDecided
		if l.sess.streamUpReader != nil {
			l.src = l.sess.streamUpReader
		} else {
			l.src = l.sess.queue
		}
	})
	return l.src.Read(b)
}

func (l *lazyStreamReader) Close() error {
	if r, ok := l.src.(io.Closer); ok {
		return r.Close()
	}
	return nil
}
