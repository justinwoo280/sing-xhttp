package xhttp

// Constructor helpers for integrating with sing-box (or any other framework
// that uses the same V2Ray transport adapter shape).
//
// This library does not import sing-box. Bridging is left to the consumer,
// which is straightforward because sing-box's adapter.V2RayServerTransport,
// V2RayClientTransport, and V2RayServerTransportHandler are structurally
// identical to our ServerTransport / ClientTransport / ServerHandler
// interfaces.
//
// Example wiring on a patched sing-box (see
// patches/0001-sing-box-register-xhttp-transport.patch):
//
//	import (
//	  "github.com/sagernet/sing-box/transport/v2ray"
//	  "github.com/justinwoo280/sing-xhttp/xhttp"
//	)
//	func init() {
//	  v2ray.RegisterXHTTP(xhttp.ServerConstructor, xhttp.ClientConstructor)
//	}

import (
	"context"

	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	aTLS "github.com/sagernet/sing/common/tls"
)

// ServerConstructor builds a ServerTransport. Signature matches what a
// patched sing-box v2ray.RegisterXHTTP expects (sing-box's tls.ServerConfig
// and adapter.V2RayServerTransportHandler are structurally compatible).
func ServerConstructor(ctx context.Context, logger logger.ContextLogger, options Options, tlsConfig aTLS.ServerConfig, handler ServerHandler) (ServerTransport, error) {
	return NewServer(ctx, logger, options, tlsConfig, handler)
}

// ClientConstructor builds a ClientTransport. Same notes as ServerConstructor.
func ClientConstructor(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options Options, tlsConfig aTLS.Config) (ClientTransport, error) {
	return NewClient(ctx, dialer, serverAddr, options, tlsConfig)
}
