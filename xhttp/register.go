package xhttp

// Register adapter functions to plug into a sing-box build that has been
// patched (see patches/0001-sing-box-register-xhttp-transport.patch) to expose
// the (transport/v2ray).RegisterXHTTP hook.
//
// We intentionally don't import sing-box's v2ray package here to keep this
// library buildable against vanilla sing-box. Users wire the registration
// from their own build's main package, e.g.:
//
//   import (
//     "github.com/sagernet/sing-box/transport/v2ray"
//     "github.com/justinwoo280/sing-xhttp/xhttp"
//   )
//   func init() {
//     v2ray.RegisterXHTTP(xhttp.ServerConstructor, xhttp.ClientConstructor)
//   }

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// ServerConstructor matches sing-box patched signature.
func ServerConstructor(ctx context.Context, logger logger.ContextLogger, options Options, tlsConfig tls.ServerConfig, handler adapter.V2RayServerTransportHandler) (adapter.V2RayServerTransport, error) {
	return NewServer(ctx, logger, options, tlsConfig, handler)
}

// ClientConstructor matches sing-box patched signature.
func ClientConstructor(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options Options, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
	return NewClient(ctx, dialer, serverAddr, options, tlsConfig)
}
