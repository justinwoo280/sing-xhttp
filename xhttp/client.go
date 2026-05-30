package xhttp

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	aTLS "github.com/sagernet/sing/common/tls"

	"github.com/gofrs/uuid/v5"
	"golang.org/x/net/http2"
)

var _ ClientTransport = (*Client)(nil)

type Client struct {
	ctx        context.Context
	dialer     N.Dialer
	serverAddr M.Socksaddr
	xmux       *xmuxManager
	http2      bool
	requestURL url.URL
	host       string
	method     string
	headers    http.Header
	opts       Options

	// derived defaults
	codec           *codec
	maxEachPost     Range
	minPostInterval Range
	maxBufferedPost int
}

func NewClient(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options Options, tlsConfig aTLS.Config) (*Client, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	// Compute H2 keep-alive period from xmux config.
	var keepAlive time.Duration = 30 * time.Second // Chrome-like default
	if options.Xmux != nil && options.Xmux.HKeepAlivePeriod != 0 {
		keepAlive = time.Duration(options.Xmux.HKeepAlivePeriod) * time.Second
		if keepAlive < 0 {
			keepAlive = 0 // explicitly disable
		}
	}
	newTransport := buildTransportFactory(dialer, tlsConfig, keepAlive)

	mode := options.Mode
	switch mode {
	case ModePacketUp, ModeStreamUp:
	case "":
		mode = ModePacketUp
	default:
		return nil, E.New("unsupported xhttp mode: ", mode)
	}
	options.Mode = mode

	// stream-up requires a flushable, bidirectional uplink. Go's net/http
	// client on HTTP/1.1 buffers chunked request bodies, so stream-up needs
	// HTTP/2 (i.e. TLS) to work correctly. packet-up is fine on both.
	if mode == ModeStreamUp && tlsConfig == nil {
		return nil, E.New("xhttp: stream-up mode requires TLS (HTTP/2)")
	}

	if options.Method == "" {
		options.Method = http.MethodPost
	}
	if !strings.HasPrefix(options.Path, "/") {
		options.Path = "/" + options.Path
	}

	// Split path and query so that ?key=val in the configured path
	// is preserved in the actual request URL.
	var rawQuery string
	if i := strings.IndexByte(options.Path, '?'); i >= 0 {
		rawQuery = options.Path[i+1:]
		options.Path = options.Path[:i]
	}

	var requestURL url.URL
	if tlsConfig == nil {
		requestURL.Scheme = "http"
	} else {
		requestURL.Scheme = "https"
	}
	requestURL.Host = serverAddr.String()
	requestURL.Path = options.Path
	requestURL.RawQuery = rawQuery

	host := options.Host
	if host == "" && tlsConfig != nil {
		host = tlsConfig.ServerName()
	}
	if host == "" {
		host = serverAddr.AddrString()
	}

	var xmuxCfg XmuxConfig
	if options.Xmux != nil {
		xmuxCfg = *options.Xmux
	}
	mgr := newXmuxManager(xmuxCfg, func() *xmuxConn {
		return &xmuxConn{transport: newTransport()}
	})

	return &Client{
		ctx:             ctx,
		dialer:          dialer,
		serverAddr:      serverAddr,
		xmux:            mgr,
		http2:           tlsConfig != nil,
		requestURL:      requestURL,
		host:            host,
		method:          options.Method,
		headers:         options.Headers.Build(),
		opts:            options,
		codec:           newCodec(options),
		maxEachPost:     options.ScMaxEachPostBytes.orDefault(1_000_000, 1_000_000),
		minPostInterval: options.ScMinPostsIntervalMs.orDefault(30, 30),
		maxBufferedPost: int(options.ScMaxBufferedPosts),
	}, nil
}

func (c *Client) Close() error {
	if c.xmux != nil {
		c.xmux.Close()
	}
	return nil
}

// buildTransportFactory returns a function that creates a fresh
// http.RoundTripper per pooled xmuxConn. Plaintext gets a stock
// http.Transport; TLS gets an http2.Transport.
func buildTransportFactory(dialer N.Dialer, tlsConfig aTLS.Config, keepAlivePeriod time.Duration) func() http.RoundTripper {
	if tlsConfig == nil {
		return func() http.RoundTripper {
			return &http.Transport{
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					return dialer.DialContext(ctx, network, M.ParseSocksaddr(addr))
				},
				ForceAttemptHTTP2:   false,
				DisableKeepAlives:   false,
				IdleConnTimeout:     300 * time.Second,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				MaxConnsPerHost:     20,
			}
		}
	}
	if len(tlsConfig.NextProtos()) == 0 {
		tlsConfig.SetNextProtos([]string{http2.NextProtoTLS})
	}
	return func() http.RoundTripper {
		return &http2.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string, cfg *aTLS.STDConfig) (net.Conn, error) {
				raw, err := dialer.DialContext(ctx, network, M.ParseSocksaddr(addr))
				if err != nil {
					return nil, err
				}
				tlsConn, err := aTLS.ClientHandshake(ctx, raw, tlsConfig)
				if err != nil {
					raw.Close()
					return nil, err
				}
				return tlsConn, nil
			},
			ReadIdleTimeout:                keepAlivePeriod,
			IdleConnTimeout:                300 * time.Second,
			MaxHeaderListSize:              10 << 20, // 10 MB
			StrictMaxConcurrentStreams:     true,
		}
	}
}

func (c *Client) DialContext(ctx context.Context) (net.Conn, error) {
	sidUUID, _ := uuid.NewV4()
	sessionID := sidUUID.String()

	xc := c.xmux.Pick()
	xc.openUsage.Add(1)
	xc.leftRequests.Add(-1)

	switch c.opts.Mode {
	case ModeStreamUp:
		return c.dialStreamUp(ctx, sessionID, xc)
	default:
		return c.dialPacketUp(ctx, sessionID, xc)
	}
}

// --- helpers ---

func (c *Client) cloneHeaders() http.Header {
	if c.headers == nil {
		return make(http.Header)
	}
	return c.headers.Clone()
}

// newRequest builds a fresh *http.Request with padding/host applied. The URL
// path embeds sessionID (and seq if non-empty).
func (c *Client) newRequest(ctx context.Context, method, sessionID, seqStr string, body io.Reader) (*http.Request, error) {
	u := c.requestURL
	u.Path = c.codec.basePath
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Host = c.host
	req.Header = c.cloneHeaders()
	c.codec.applyMetaToRequest(req, sessionID, seqStr)
	c.codec.applyPaddingToRequest(req)
	return req, nil
}

// openDownload opens the long-lived GET that carries the downlink. The
// returned ReadCloser is the response body and lives for the session.
func (c *Client) openDownload(ctx context.Context, sessionID string, xc *xmuxClient) (io.ReadCloser, net.Addr, net.Addr, error) {
	req, err := c.newRequest(ctx, http.MethodGet, sessionID, "", nil)
	if err != nil {
		return nil, nil, nil, err
	}
	resp, err := xc.conn.transport.RoundTrip(req)
	if err != nil {
		return nil, nil, nil, E.Cause(err, "xhttp: open download")
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, nil, nil, E.New("xhttp: download bad status: ", resp.Status)
	}
	return resp.Body, c.serverAddr.TCPAddr(), nil, nil
}

// --- stream-up ---

func (c *Client) dialStreamUp(ctx context.Context, sessionID string, xc *xmuxClient) (net.Conn, error) {
	downBody, remote, local, err := c.openDownload(ctx, sessionID, xc)
	if err != nil {
		xc.openUsage.Add(-1)
		return nil, err
	}

	pr, pw := io.Pipe()
	upReq, err := c.newRequest(ctx, c.method, sessionID, "", pr)
	if err != nil {
		downBody.Close()
		return nil, err
	}
	if !c.opts.NoGRPCHeader {
		upReq.Header.Set("Content-Type", "application/grpc")
	}

	doneOnce := atomic.Bool{}
	closeAll := func() error {
		if doneOnce.Swap(true) {
			return nil
		}
		xc.openUsage.Add(-1)
		_ = pw.Close()
		return downBody.Close()
	}

	go func() {
		resp, err := xc.conn.transport.RoundTrip(upReq)
		if err != nil {
			_ = pw.CloseWithError(err)
			_ = downBody.Close()
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			_ = pw.CloseWithError(E.New("xhttp: upload bad status: ", resp.Status))
			_ = downBody.Close()
		}
	}()

	return &splitConn{
		reader:  downBody,
		writer:  pw,
		local:   local,
		remote:  remote,
		onClose: closeAll,
	}, nil
}

// --- packet-up ---

// maxConcurrentPosts limits the number of in-flight packet-up POST
// goroutines per session. For HTTP/2, the transport multiplexes them
// onto a single connection; for HTTP/1.1, the transport serializes
// them via connection pooling. This prevents goroutine explosion when
// the application writes faster than the network can send.
const maxConcurrentPosts = 32

func (c *Client) dialPacketUp(ctx context.Context, sessionID string, xc *xmuxClient) (net.Conn, error) {
	downBody, remote, local, err := c.openDownload(ctx, sessionID, xc)
	if err != nil {
		xc.openUsage.Add(-1)
		return nil, err
	}

	maxEach := int(rangeRand(c.maxEachPost))
	if maxEach < 1 {
		maxEach = 1
	}

	// batchAccumulator batches small writes into larger POST chunks.
	acc := newBatchAccumulator(maxEach)

	uctx, cancel := context.WithCancel(ctx)
	closeOnce := atomic.Bool{}
	closeAll := func() error {
		if closeOnce.Swap(true) {
			return nil
		}
		cancel()
		xc.openUsage.Add(-1)
		_ = acc.Close()
		return downBody.Close()
	}

	go c.runPacketUploader(uctx, sessionID, acc, xc, closeAll)

	return &splitConn{
		reader:  downBody,
		writer:  acc,
		local:   local,
		remote:  remote,
		onClose: closeAll,
	}, nil
}

// runPacketUploader drains batched chunks from the accumulator and sends
// them as concurrent POST requests. Sequence numbers are assigned in the
// main loop (single-threaded), ensuring correct ordering at the server's
// uploadQueue. The POSTs themselves run concurrently via goroutines,
// bounded by a semaphore of size maxConcurrentPosts.
func (c *Client) runPacketUploader(
	ctx context.Context,
	sessionID string,
	acc *batchAccumulator,
	xc *xmuxClient,
	closeAll func() error,
) {
	var (
		seq    uint64
		wg     sync.WaitGroup
		failed atomic.Bool
	)

	sem := make(chan struct{}, maxConcurrentPosts)
	defer wg.Wait()

	// When the pool spans multiple connections, fan this session's POSTs
	// across them so per-connection pacing intervals overlap in parallel.
	// The session stays bound to xc for concurrency bookkeeping; the extra
	// connections are only borrowed to carry POSTs, so we don't touch their
	// openUsage/leftUsage counters here.
	spread := c.xmux.poolsConnections()
	var fanout []*xmuxClient
	var fanIdx int
	if spread {
		fanout = c.xmux.liveClients()
	}

	for {
		if failed.Load() {
			return
		}

		chunk, err := acc.Drain()
		if err != nil || len(chunk) == 0 {
			return
		}

		// Re-check after Drain in case a POST failed while we were blocked.
		if failed.Load() {
			return
		}

		// Rotate the bound connection when it hits its lifetime caps. Pick()
		// sweeps any connection past its request/time budget.
		if xc.leftRequests.Add(-1) <= 0 ||
			(!xc.unreusableAt.IsZero() && time.Now().After(xc.unreusableAt)) {
			newXc := c.xmux.Pick()
			newXc.openUsage.Add(1)
			xc.openUsage.Add(-1)
			xc = newXc
			if spread {
				fanout = c.xmux.liveClients()
			}
		}

		// Choose the connection this POST rides on: round-robin over the pool
		// when spreading, otherwise the session's bound connection.
		postXc := xc
		if spread && len(fanout) > 0 {
			postXc = fanout[fanIdx%len(fanout)]
			fanIdx++
		}

		seqStr := strconv.FormatUint(seq, 10)
		seq++

		// Acquire concurrency slot.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		wg.Add(1)
		go func(seqStr string, payload []byte, postXc *xmuxClient) {
			defer func() {
				<-sem
				wg.Done()
			}()

			// Per-connection minimum spacing. Reserving a slot returns the
			// wait this POST owes on its connection; different connections
			// wait independently, so the pool sends in parallel.
			if wait := postXc.reservePostSlot(c.minPostInterval); wait > 0 {
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return
				}
			}

			if postErr := c.sendOnePost(ctx, sessionID, seqStr, payload, postXc); postErr != nil {
				failed.Store(true)
				_ = acc.CloseWithError(postErr)
				_ = closeAll()
			}
		}(seqStr, chunk, postXc)
	}
}

func (c *Client) sendOnePost(ctx context.Context, sessionID, seqStr string, payload []byte, xc *xmuxClient) error {
	req, err := c.newRequest(ctx, c.method, sessionID, seqStr, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(payload))
	resp, err := xc.conn.transport.RoundTrip(req)
	if err != nil {
		return E.Cause(err, "xhttp: post packet")
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return E.New("xhttp: post bad status: ", resp.Status)
	}
	return nil
}
