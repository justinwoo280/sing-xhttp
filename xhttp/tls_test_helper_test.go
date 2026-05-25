package xhttp_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptoRand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"time"

	aTLS "github.com/sagernet/sing/common/tls"
)

// makeTLSPair returns matched (server, client) TLS configs using a fresh
// self-signed certificate for "localhost". Both expose ALPN "h2".
func makeTLSPair(t testingTB) (*serverTLS, *clientTLS) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), cryptoRand.Reader)
	if err != nil { t.Fatal(err) }
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(cryptoRand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil { t.Fatal(err) }
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}

	pool := x509.NewCertPool()
	parsed, _ := x509.ParseCertificate(der)
	pool.AddCert(parsed)

	serverCfg := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}}
	clientCfg := &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"h2"}}
	return &serverTLS{cfg: serverCfg}, &clientTLS{cfg: clientCfg}
}

type testingTB interface {
	Helper()
	Fatal(args ...any)
}

// --- client side ---

type clientTLS struct {
	mu  sync.Mutex
	cfg *tls.Config
}

func (c *clientTLS) ServerName() string                          { return c.cfg.ServerName }
func (c *clientTLS) SetServerName(s string)                      { c.cfg.ServerName = s }
func (c *clientTLS) NextProtos() []string                        { return c.cfg.NextProtos }
func (c *clientTLS) SetNextProtos(p []string)                    { c.cfg.NextProtos = p }
func (c *clientTLS) STDConfig() (*tls.Config, error) { return c.cfg.Clone(), nil }
func (c *clientTLS) Config() (*tls.Config, error)             { return c.cfg.Clone(), nil }
func (c *clientTLS) Client(conn net.Conn) (aTLS.Conn, error)     { return &stdTLSConn{Conn: tls.Client(conn, c.cfg.Clone()), under: conn}, nil }
func (c *clientTLS) Clone() aTLS.Config                          { return &clientTLS{cfg: c.cfg.Clone()} }

// --- server side ---

type serverTLS struct {
	mu  sync.Mutex
	cfg *tls.Config
}

func (s *serverTLS) ServerName() string                          { return "" }
func (s *serverTLS) SetServerName(string)                        {}
func (s *serverTLS) NextProtos() []string                        { return s.cfg.NextProtos }
func (s *serverTLS) SetNextProtos(p []string)                    { s.cfg.NextProtos = p }
func (s *serverTLS) STDConfig() (*tls.Config, error) { return s.cfg.Clone(), nil }
func (s *serverTLS) Config() (*tls.Config, error)             { return s.cfg.Clone(), nil }
func (s *serverTLS) Client(conn net.Conn) (aTLS.Conn, error)     { return &stdTLSConn{Conn: tls.Client(conn, s.cfg.Clone()), under: conn}, nil }
func (s *serverTLS) Clone() aTLS.Config                          { return &serverTLS{cfg: s.cfg.Clone()} }
func (s *serverTLS) Start() error                                { return nil }
func (s *serverTLS) Close() error                                { return nil }
func (s *serverTLS) Server(conn net.Conn) (aTLS.Conn, error)     { return &stdTLSConn{Conn: tls.Server(conn, s.cfg.Clone()), under: conn}, nil }

// --- tls.Conn wrapper to implement aTLS.Conn ---

type stdTLSConn struct {
	*tls.Conn
	under net.Conn
}

func (c *stdTLSConn) NetConn() net.Conn                                  { return c.under }
func (c *stdTLSConn) HandshakeContext(ctx context.Context) error         { return c.Conn.HandshakeContext(ctx) }
func (c *stdTLSConn) ConnectionState() aTLS.ConnectionState              { return c.Conn.ConnectionState() }
