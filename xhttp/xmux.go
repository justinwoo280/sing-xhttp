package xhttp

import (
	cryptoRand "crypto/rand"
	"math"
	"math/big"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// xmuxConn is a single pooled HTTP transport. It is the unit the XmuxManager
// hands out: when picked, all of a session's GET + POSTs go through this
// transport's RoundTripper, which is backed by its own TCP/TLS session(s).
type xmuxConn struct {
	transport http.RoundTripper
	closed    atomic.Bool
}

// IsClosed reports whether this xmuxConn has been retired. A retired conn
// is dropped from the pool on the next Pick.
func (x *xmuxConn) IsClosed() bool { return x.closed.Load() }

// Close marks the conn retired and calls CloseIdleConnections on the
// underlying transport (releasing any pooled sockets).
func (x *xmuxConn) Close() {
	if x.closed.Swap(true) {
		return
	}
	if ci, ok := x.transport.(interface{ CloseIdleConnections() }); ok {
		ci.CloseIdleConnections()
	}
}

// xmuxClient wraps a pooled conn with the bookkeeping needed for the
// concurrency / reuse / time / request-count limits.
type xmuxClient struct {
	conn         *xmuxConn
	openUsage    atomic.Int32 // currently-in-flight sessions
	leftUsage    int32        // remaining Pick() returns; -1 = unlimited (protected by manager mu)
	leftRequests atomic.Int32 // remaining sessions before retirement; MaxInt32 = unlimited
	unreusableAt time.Time    // zero = never expire

	postMu     sync.Mutex // guards nextPostAt
	nextPostAt time.Time  // earliest wall-clock time the next POST on this conn may start
}

// reservePostSlot enforces a per-connection minimum spacing between POSTs.
// It atomically advances this connection's nextPostAt by a randomized
// interval and returns how long the caller should wait before sending.
//
// Spacing is per-connection rather than global so that POSTs on different
// xmux connections proceed in parallel: each connection still presents the
// anti-burst jitter the interval is meant to provide, but aggregate
// throughput scales with the pool size instead of being globally serialized.
func (x *xmuxClient) reservePostSlot(interval Range) time.Duration {
	if interval.From <= 0 {
		return 0
	}
	gap := time.Duration(rangeRand(interval)) * time.Millisecond
	x.postMu.Lock()
	defer x.postMu.Unlock()
	now := time.Now()
	if x.nextPostAt.IsZero() || now.After(x.nextPostAt) {
		x.nextPostAt = now.Add(gap)
		return 0
	}
	wait := x.nextPostAt.Sub(now)
	x.nextPostAt = x.nextPostAt.Add(gap)
	return wait
}

// xmuxManager picks an xmuxClient per session, creating new connections up
// to MaxConnections and retiring them when their limits are exhausted.
type xmuxManager struct {
	mu          sync.Mutex
	config      XmuxConfig
	concurrency int32 // realized from config.MaxConcurrency for this manager's lifetime
	connections int32 // realized from config.MaxConnections
	newConn     func() *xmuxConn
	clients     []*xmuxClient
}

func newXmuxManager(cfg XmuxConfig, newConn func() *xmuxConn) *xmuxManager {
	return &xmuxManager{
		config:      cfg,
		concurrency: rangeRand(orZero(cfg.MaxConcurrency)),
		connections: rangeRand(orZero(cfg.MaxConnections)),
		newConn:     newConn,
		clients:     make([]*xmuxClient, 0),
	}
}

func orZero(r *Range) Range {
	if r == nil {
		return Range{}
	}
	return *r
}

func (m *xmuxManager) newClient() *xmuxClient {
	c := &xmuxClient{
		conn:      m.newConn(),
		leftUsage: -1,
	}
	if x := rangeRand(orZero(m.config.CMaxReuseTimes)); x > 0 {
		c.leftUsage = x - 1
	}
	c.leftRequests.Store(math.MaxInt32)
	if x := rangeRand(orZero(m.config.HMaxRequestTimes)); x > 0 {
		c.leftRequests.Store(x)
	}
	if x := rangeRand(orZero(m.config.HMaxReusableSecs)); x > 0 {
		c.unreusableAt = time.Now().Add(time.Duration(x) * time.Second)
	}
	m.clients = append(m.clients, c)
	return c
}

// Pick returns the xmuxClient that should serve the next session. Caller must
// .openUsage.Add(1) before use and .openUsage.Add(-1) when done; the caller
// is also responsible for decrementing .leftRequests once per session.
func (m *xmuxManager) Pick() *xmuxClient {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Sweep dead clients.
	for i := 0; i < len(m.clients); {
		c := m.clients[i]
		retired := c.conn.IsClosed() ||
			c.leftUsage == 0 ||
			c.leftRequests.Load() <= 0 ||
			(!c.unreusableAt.IsZero() && time.Now().After(c.unreusableAt))
		if retired {
			c.conn.Close()
			m.clients = append(m.clients[:i], m.clients[i+1:]...)
			continue
		}
		i++
	}

	// Empty pool: spawn one.
	if len(m.clients) == 0 {
		return m.newClient()
	}
	// Below cap: spawn another.
	if m.connections > 0 && len(m.clients) < int(m.connections) {
		return m.newClient()
	}

	// Filter by concurrency budget.
	var candidates []*xmuxClient
	if m.concurrency > 0 {
		for _, c := range m.clients {
			if c.openUsage.Load() < m.concurrency {
				candidates = append(candidates, c)
			}
		}
	} else {
		candidates = m.clients
	}

	// All saturated: spawn a fresh client even past MaxConnections.
	if len(candidates) == 0 {
		return m.newClient()
	}

	idx := randIndex(len(candidates))
	picked := candidates[idx]
	if picked.leftUsage > 0 {
		picked.leftUsage--
	}
	return picked
}

// Close retires all pooled connections.
func (m *xmuxManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.clients {
		c.conn.Close()
	}
	m.clients = nil
}

// poolsConnections reports whether this manager is configured to spread work
// over more than one connection. MaxConnections == 0 means "unlimited", which
// in practice still behaves as a single connection until saturated, so it is
// not treated as a multi-connection pool here.
func (m *xmuxManager) poolsConnections() bool {
	return m.connections > 1
}

// liveClients returns a snapshot of the currently-pooled, non-retired clients.
// Used by the packet uploader to fan a single session's POSTs across the pool
// for parallel pacing, without disturbing the session-level concurrency
// bookkeeping that Pick() maintains.
func (m *xmuxManager) liveClients() []*xmuxClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*xmuxClient, 0, len(m.clients))
	for _, c := range m.clients {
		if !c.conn.IsClosed() {
			out = append(out, c)
		}
	}
	return out
}

func randIndex(n int) int {
	if n <= 1 {
		return 0
	}
	v, err := cryptoRand.Int(cryptoRand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0
	}
	return int(v.Int64())
}
