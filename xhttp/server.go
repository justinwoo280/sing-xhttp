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

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/http3"
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
	h3Server   *http3.Server
	quicConfig *quic.Config

	host    string
	path    string
	method  string // "" means accept any uplink method (default POST)
	headers http.Header
	opts    Options

	codec                *codec
	maxEachPostBytes     Range
	maxBufferedPosts     int
	streamUpServerSecs   Range
	serverMaxHeaderBytes int

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
	closed         chan struct{} // closed once the session is torn down
	closeOnce      sync.Once
}

// Close tears the session down, unblocking any reader: it closes the packet-up
// queue, signals the closed channel (so a reader still waiting for the uplink
// mode gives up), and closes the stream-up body if one was attached.
func (s *httpSession) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		_ = s.queue.Close()
		// Read streamUpReader only after uplinkDecided is observed closed: the
		// write in decideStreamUp happens-before that close, so this is race-free.
		select {
		case <-s.uplinkDecided:
			if s.streamUpReader != nil {
				_ = s.streamUpReader.Close()
			}
		default:
		}
	})
	return nil
}

func (s *httpSession) decideStreamUp(body io.ReadCloser) bool {
	done := false
	s.uplinkOnce.Do(func() {
		s.streamUpReader = body
		close(s.uplinkDecided)
		done = true
	})
	if done {
		select {
		case <-s.closed:
			_ = body.Close()
			return false
		default:
		}
	}
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
	maxHeaderBytes := int(options.ServerMaxHeaderBytes)
	if maxHeaderBytes <= 0 {
		maxHeaderBytes = 8192
	}
	md := defaultsForMode(options.Mode)
	s := &Server{
		ctx:                  ctx,
		logger:               logger,
		tlsConfig:            tlsConfig,
		handler:              handler,
		h2Server:             &http2.Server{},
		host:                 options.Host,
		path:                 options.Path,
		method:               options.Method,
		headers:              options.Headers.Build(),
		opts:                 options,
		codec:                newCodec(options),
		maxEachPostBytes:     options.ScMaxEachPostBytes.orModeDefault(md.maxEachPostBytes),
		maxBufferedPosts:     intOr(options.ScMaxBufferedPosts, int(md.maxBufferedPosts)),
		streamUpServerSecs:   options.ScStreamUpServerSecs.orModeDefault(md.streamUpServerSecs),
		serverMaxHeaderBytes: maxHeaderBytes,
		sessions:             make(map[string]*httpSession),
	}
	s.httpServer = &http.Server{
		Handler:           s,
		ReadHeaderTimeout: tcpReadHeaderTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
			return contextWithNewConnID(ctx)
		},
	}
	s.h2cHandler = h2c.NewHandler(s, s.h2Server)
	// HTTP/3: when TLS config requests h3 as the only NextProto, use QUIC.
	if tlsConfig != nil && len(tlsConfig.NextProtos()) == 1 && tlsConfig.NextProtos()[0] == "h3" {
		s.quicConfig = &quic.Config{
			DisablePathMTUDiscovery: true, // safe default for non-Linux/Windows
		}
		s.h3Server = &http3.Server{Handler: s}
	}
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

func (s *Server) Network() []string {
	if s.h3Server != nil {
		return []string{N.NetworkUDP}
	}
	return []string{N.NetworkTCP}
}

func (s *Server) Serve(listener net.Listener) error {
	if s.h3Server != nil {
		// HTTP/3: use the QUIC listener (ServePacket handles this)
		return os.ErrInvalid
	}
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

func (s *Server) ServePacket(listener net.PacketConn) error {
	if s.h3Server == nil {
		return os.ErrInvalid
	}
	// HTTP/3: get *tls.Config from the server config, then serve HTTP/3.
	gotlsConfig, err := s.tlsConfig.STDConfig()
	if err != nil {
		return E.Cause(err, "xhttp: get TLS config for HTTP/3")
	}
	quicListener, err := quic.ListenEarly(listener, gotlsConfig, s.quicConfig)
	if err != nil {
		return E.Cause(err, "xhttp: QUIC listen")
	}
	return s.h3Server.ServeListener(quicListener)
}

func (s *Server) Close() error {
	if s.h3Server != nil {
		return common.Close(common.PtrOrNil(s.h3Server))
	}
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
		closed:        make(chan struct{}),
	}
	s.sessions[sessionID] = sess
	// reap if GET never arrives within 30s
	go func() {
		select {
		case <-time.After(30 * time.Second):
		case <-sess.connected:
			return
		case <-sess.closed:
		}
		s.sessionsMu.Lock()
		if s.sessions[sessionID] == sess {
			delete(s.sessions, sessionID)
		}
		s.sessionsMu.Unlock()
		_ = sess.Close()
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

	if s.host != "" && !isValidHTTPHost(r.Host, s.host) {
		s.invalid(w, r, http.StatusNotFound, E.New("bad host: ", r.Host))
		return
	}
	if !strings.HasPrefix(r.URL.Path, s.codec.basePath) {
		s.invalid(w, r, http.StatusNotFound, E.New("bad path: ", r.URL.Path))
		return
	}

	// Apply common response headers / CORS.
	s.writeCORSHeaders(w, r)
	w.Header().Set("Cache-Control", "no-store")
	for k, vs := range s.headers {
		for _, v := range vs {
			w.Header().Set(k, v)
		}
	}
	s.codec.applyPaddingToResponseHeader(w)

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Validate x_padding to prevent probing.
	paddingValue := s.codec.extractPaddingFromRequest(r)
	if !s.codec.validatePadding(paddingValue) {
		s.invalid(w, r, http.StatusBadRequest, E.New("invalid x_padding"))
		return
	}
	obfsPaddingAccepted := s.codec.xpadObfs && paddingValue != ""
	if s.opts.GRPCFraming && r.Method != http.MethodGet {
		if r.Method != http.MethodPost || r.ProtoMajor < 2 || r.URL.Path != grpcPath(s.codec.basePath) {
			s.invalid(w, r, http.StatusNotFound, E.New("invalid gRPC method or path"))
			return
		}
		if !isGRPCContentType(r.Header.Get("Content-Type")) {
			s.invalid(w, r, http.StatusUnsupportedMediaType, E.New("gRPC content type required"))
			return
		}
		if encoding := r.Header.Get("Grpc-Encoding"); encoding != "" && encoding != "identity" {
			s.invalid(w, r, http.StatusUnsupportedMediaType, E.New("unsupported gRPC encoding"))
			return
		}
		sessionID := r.Header.Get(grpcSessionHeader)
		if sessionID == "" && s.modeAllows(ModeStreamOne) {
			s.handleStreamOne(w, r)
		} else if sessionID != "" && s.modeAllows(ModeStreamUp) {
			s.handleStreamUpPost(w, r, sessionID, obfsPaddingAccepted)
		} else {
			s.invalid(w, r, http.StatusBadRequest, E.New("gRPC stream mode not allowed"))
		}
		return
	}

	sessionID, seqStr, ok := s.codec.extractMetaFromRequest(r)
	if !ok {
		s.invalid(w, r, http.StatusNotFound, E.New("path doesn't match base"))
		return
	}
	if s.opts.GRPCFraming && (sessionID == "" || seqStr != "") {
		s.invalid(w, r, http.StatusBadRequest, E.New("gRPC stream-up download requires a session ID without sequence"))
		return
	}

	// stream-one: no session ID, any method with body or GET without session.
	if sessionID == "" && s.modeAllows(ModeStreamOne) {
		s.handleStreamOne(w, r)
		return
	}

	isUplink := r.Method != http.MethodGet || seqStr != ""
	if isUplink && sessionID == "" {
		s.invalid(w, r, http.StatusBadRequest, E.New("upload without sessionId"))
		return
	}

	switch {
	case isUplink && seqStr != "":
		if !s.modeAllows(ModePacketUp) {
			s.invalid(w, r, http.StatusBadRequest, E.New("packet-up not allowed"))
			return
		}
		s.handlePacketUpPost(w, r, sessionID, seqStr)
	case isUplink && seqStr == "":
		if !s.modeAllows(ModeStreamUp) {
			s.invalid(w, r, http.StatusBadRequest, E.New("stream-up not allowed"))
			return
		}
		s.handleStreamUpPost(w, r, sessionID, obfsPaddingAccepted)
	default:
		s.handleDownloadGet(w, r, sessionID)
	}
}

// modeAllows reports whether the configured server mode allows the given
// request-uplink mode. Matches Xray's server-side checks: the server only
// distinguishes three uplink shapes — stream-one (no session), stream-up
// (session, no seq), packet-up (session + seq). "auto"/"" accept everything.
//
// stream-down is a client-only concept: on the wire the server sees a
// packet-up uplink (POST + seq) plus a download GET, so a server configured
// as stream-down must accept packet-up uplinks (and its download GET).
func (s *Server) modeAllows(requestMode string) bool {
	m := s.opts.Mode
	if m == "" || m == ModeAuto {
		return true
	}
	if m == ModeStreamDown {
		// stream-down server accepts packet-up uplinks + downloads.
		return requestMode == ModePacketUp
	}
	return m == requestMode
}

// writeCORSHeaders sets CORS headers matching Xray's behavior: reflect the
// Origin header (or "*" if absent), set Allow-Credentials when cookie
// placement is configured, and handle OPTIONS preflight properly.
func (s *Server) writeCORSHeaders(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	} else {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}

	needsCredentials := s.codec.sessionPlacement == PlacementCookie ||
		s.codec.seqPlacement == PlacementCookie ||
		s.codec.xpadPlacement == PlacementCookie ||
		s.codec.uplinkDataPlacement == PlacementCookie ||
		s.codec.uplinkDataPlacement == PlacementAuto
	if needsCredentials {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	if r.Method == http.MethodOptions {
		requestedMethod := r.Header.Get("Access-Control-Request-Method")
		if requestedMethod != "" {
			w.Header().Set("Access-Control-Allow-Methods", requestedMethod)
		} else {
			w.Header().Set("Access-Control-Allow-Methods", "*")
		}
		requestedHeaders := r.Header.Get("Access-Control-Request-Headers")
		if requestedHeaders != "" {
			w.Header().Set("Access-Control-Allow-Headers", requestedHeaders)
		} else {
			w.Header().Set("Access-Control-Allow-Headers", "*")
		}
	}
}

func (s *Server) handlePacketUpPost(w http.ResponseWriter, r *http.Request, sessionID, seqStr string) {
	max := int(s.maxEachPostBytes.To)
	if max <= 0 {
		max = 1_000_000
	}
	if r.ContentLength > int64(max) {
		s.invalid(w, r, http.StatusRequestEntityTooLarge, E.New("upload too large"))
		return
	}

	// Read body payload (needed for body and auto placements).
	var bodyPayload []byte
	if s.codec.uplinkDataPlacement == PlacementBody ||
		s.codec.uplinkDataPlacement == PlacementAuto ||
		s.codec.uplinkDataPlacement == "" {
		if r.ContentLength > 0 {
			bodyPayload = make([]byte, r.ContentLength)
			if _, err := io.ReadFull(r.Body, bodyPayload); err != nil {
				s.invalid(w, r, http.StatusBadRequest, E.Cause(err, "read body"))
				return
			}
		} else if s.codec.uplinkDataPlacement == PlacementBody || s.codec.uplinkDataPlacement == "" {
			var buf bytes.Buffer
			lim := io.LimitReader(r.Body, int64(max)+1)
			if _, err := io.Copy(&buf, lim); err != nil {
				s.invalid(w, r, http.StatusBadRequest, E.Cause(err, "read body"))
				return
			}
			if buf.Len() > max {
				s.invalid(w, r, http.StatusRequestEntityTooLarge, E.New("upload too large"))
				return
			}
			bodyPayload = buf.Bytes()
		}
	}

	payload := s.codec.decodeUplinkPayload(r, bodyPayload)
	if len(payload) > max {
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
	if err := sess.queue.Push(packet{payload: payload, seq: seq}); err != nil {
		s.invalid(w, r, http.StatusConflict, err)
		return
	}
	if len(bodyPayload) == 0 {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleStreamUpPost(w http.ResponseWriter, r *http.Request, sessionID string, obfsPaddingAccepted bool) {
	sess := s.upsertSession(sessionID)
	body := r.Body
	var framedReader *grpcReader
	if s.opts.GRPCFraming {
		framedReader = newGRPCReader(body)
		body = framedReader
	}
	if !sess.decideStreamUp(body) {
		s.invalid(w, r, http.StatusConflict, E.New("uplink already attached"))
		return
	}
	defer sess.Close()
	if framedReader != nil {
		startGRPCResponse(w)
		defer func() {
			_ = sess.Close()
			framedReader.finish(w)
		}()
	}
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	// Optional reverse heartbeat to keep CDN from killing the long POST.
	// Matches Xray: heartbeat fires when Referer is present (legacy compat
	// marker) or when obfs padding was accepted.
	hasLegacyReferer := r.Header.Get("Referer") != ""
	var heartbeat <-chan time.Time
	var timer *time.Timer
	if (hasLegacyReferer || obfsPaddingAccepted) && s.streamUpServerSecs.To > 0 {
		timer = time.NewTimer(time.Duration(rangeRand(s.streamUpServerSecs)) * time.Second)
		defer timer.Stop()
		heartbeat = timer.C
	}
	flusher, _ := w.(http.Flusher)
	var writer io.WriteCloser = &flushWriter{w: w, flusher: flusher}
	if s.opts.GRPCFraming {
		writer = newGRPCWriter(writer)
	}
	defer writer.Close()
	// Write heartbeats in the handler itself so no goroutine can write after
	// trailers/END_STREAM. The upload also ends when its download session ends.
	for {
		select {
		case <-r.Context().Done():
			return
		case <-sess.closed:
			return
		case <-heartbeat:
			if _, err := writer.Write(bytes.Repeat([]byte{'X'}, int(rangeRand(s.codec.xpadRange)))); err != nil {
				return
			}
			timer.Reset(time.Duration(rangeRand(s.streamUpServerSecs)) * time.Second)
		}
	}
}

// handleStreamOne handles the stream-one mode: a single bidirectional HTTP
// stream with no session ID (REALITY-style). The request body is the uplink
// and the response body is the downlink.
func (s *Server) handleStreamOne(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Cache-Control", "no-store")
	reader := r.Body
	if s.opts.GRPCFraming {
		framedReader := newGRPCReader(reader)
		reader = framedReader
		startGRPCResponse(w)
		defer framedReader.finish(w)
	} else if !s.opts.NoSSEHeader {
		w.Header().Set("Content-Type", "text/event-stream")
	}
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	var writer io.WriteCloser = &flushWriter{w: w, flusher: flusher}
	if s.opts.GRPCFraming {
		writer = newGRPCWriter(writer)
	}
	done := make(chan struct{})
	conn := &splitConn{
		reader:  reader,
		writer:  writer,
		local:   nil,
		remote:  parseRemote(r),
		onClose: func() error { close(done); return nil },
	}

	ctx := r.Context()
	finished := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// Close both halves before the HTTP/2 handler returns. Closing only
			// the request body leaves an upper-layer writer able to race with
			// net/http's implicit END_STREAM on the response.
			_ = conn.Close()
		case <-finished:
		}
	}()

	source := sHttp.SourceAddress(r)
	s.handler.NewConnectionEx(ctx, conn, source, M.Socksaddr{}, func(error) { _ = conn.Close() })

	select {
	case <-ctx.Done():
	case <-done:
	}
	// Join any concurrent close before publishing trailers / END_STREAM.
	_ = conn.Close()
	close(finished)
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

	// NewConnectionEx may either return immediately (async handler) or block for
	// the whole connection lifetime (e.g. VLESS mux runs its read loop inline).
	// In the blocking case the reader only unblocks once the session is torn
	// down, so a watcher closes the session when the request context is
	// cancelled (client GET ends or server shuts down); otherwise that
	// NewConnectionEx would never return and the read goroutine would leak.
	ctx := r.Context()
	finished := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			if sess != nil {
				_ = sess.Close()
			}
		case <-finished:
		}
	}()

	source := sHttp.SourceAddress(r)
	s.handler.NewConnectionEx(ctx, conn, source, M.Socksaddr{}, nil)

	// Wait until the connection is really done (async handlers return before
	// the upper layer closes the conn); blocking handlers fall through at once.
	select {
	case <-ctx.Done():
	case <-done:
	}
	// A ResponseWriter is only valid until this handler returns. Closing the
	// split connection also closes flushWriter under its mutex, waiting for any
	// in-flight Write/Flush and rejecting writes from an async upper layer.
	_ = conn.Close()
	close(finished)
	if sess != nil {
		_ = sess.Close()
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
	// Prefer the real client IP from X-Forwarded-For (set by CDNs / reverse
	// proxies). The first entry is the originating client.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first := xff
		if i := strings.IndexByte(xff, ','); i >= 0 {
			first = xff[:i]
		}
		first = strings.TrimSpace(first)
		if ip := net.ParseIP(first); ip != nil {
			return &net.TCPAddr{IP: ip, Port: 0}
		}
	}
	if a, err := net.ResolveTCPAddr("tcp", r.RemoteAddr); err == nil {
		return a
	}
	return &net.TCPAddr{}
}

// isValidHTTPHost compares a request Host against the configured host,
// stripping any :port suffix from the request (matches Xray/sing-box).
func isValidHTTPHost(requestHost, configHost string) bool {
	r := strings.ToLower(requestHost)
	c := strings.ToLower(configHost)
	if strings.Contains(r, ":") {
		if h, _, err := net.SplitHostPort(r); err == nil {
			return h == c
		}
	}
	return r == c
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
		select {
		case <-l.sess.uplinkDecided:
			if l.sess.streamUpReader != nil {
				l.src = l.sess.streamUpReader
			} else {
				l.src = l.sess.queue
			}
		case <-l.sess.closed:
			l.src = eofReader{}
		}
	})
	return l.src.Read(b)
}

func (l *lazyStreamReader) Close() error {
	return l.sess.Close()
}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }
