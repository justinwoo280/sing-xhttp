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
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/gofrs/uuid/v5"
	"golang.org/x/net/http2"
)

var _ adapter.V2RayClientTransport = (*Client)(nil)

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

func NewClient(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options Options, tlsConfig tls.Config) (*Client, error) {
	newTransport := buildTransportFactory(dialer, tlsConfig)

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

	var requestURL url.URL
	if tlsConfig == nil {
		requestURL.Scheme = "http"
	} else {
		requestURL.Scheme = "https"
	}
	requestURL.Host = serverAddr.String()
	requestURL.Path = options.Path

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

// buildTransportFactory returns a function that creates a fresh http.RoundTripper
// per pooled xmuxConn. Plaintext gets a stock http.Transport; TLS gets an
// http2.Transport.
func buildTransportFactory(dialer N.Dialer, tlsConfig tls.Config) func() http.RoundTripper {
	if tlsConfig == nil {
		return func() http.RoundTripper {
			return &http.Transport{
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					return dialer.DialContext(ctx, network, M.ParseSocksaddr(addr))
				},
				ForceAttemptHTTP2: false,
			}
		}
	}
	if len(tlsConfig.NextProtos()) == 0 {
		tlsConfig.SetNextProtos([]string{http2.NextProtoTLS})
	}
	tlsDialer := tls.NewDialer(dialer, tlsConfig)
	return func() http.RoundTripper {
		return &http2.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.STDConfig) (net.Conn, error) {
				return tlsDialer.DialTLSContext(ctx, M.ParseSocksaddr(addr))
			},
			ReadIdleTimeout: 0,
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

func (c *Client) dialPacketUp(ctx context.Context, sessionID string, xc *xmuxClient) (net.Conn, error) {
	downBody, remote, local, err := c.openDownload(ctx, sessionID, xc)
	if err != nil {
		xc.openUsage.Add(-1)
		return nil, err
	}

	// Internal pipe: upper layer writes -> we batch & split into POSTs.
	pr, pw := io.Pipe()
	maxEach := int(rangeRand(c.maxEachPost))
	if maxEach < 1 {
		maxEach = 1
	}

	uctx, cancel := context.WithCancel(ctx)
	closeOnce := atomic.Bool{}
	closeAll := func() error {
		if closeOnce.Swap(true) {
			return nil
		}
		cancel()
		xc.openUsage.Add(-1)
		_ = pw.Close()
		return downBody.Close()
	}

	go c.runPacketUploader(uctx, sessionID, pr, maxEach, downBody, pw, xc)

	return &splitConn{
		reader:  downBody,
		writer:  pw,
		local:   local,
		remote:  remote,
		onClose: closeAll,
	}, nil
}

func (c *Client) runPacketUploader(ctx context.Context, sessionID string, pr *io.PipeReader, maxEach int, downBody io.Closer, pw *io.PipeWriter, xc *xmuxClient) {
	var seq uint64
	var lastWrite time.Time

	buffer := make([]byte, maxEach)
	for {
		n, err := pr.Read(buffer)
		if n > 0 {
			// Optional minimum spacing between POSTs.
			if c.minPostInterval.From > 0 {
				wait := time.Duration(rangeRand(c.minPostInterval))*time.Millisecond - time.Since(lastWrite)
				if wait > 0 {
					select {
					case <-time.After(wait):
					case <-ctx.Done():
						return
					}
				}
			}
			lastWrite = time.Now()

			chunk := make([]byte, n)
			copy(chunk, buffer[:n])
			seqStr := strconv.FormatUint(seq, 10)
			seq++

			if postErr := c.sendOnePost(ctx, sessionID, seqStr, chunk, xc); postErr != nil {
				_ = pw.CloseWithError(postErr)
				_ = downBody.Close()
				return
			}
		}
		if err != nil {
			return
		}
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
