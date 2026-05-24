package xhttp

import (
	"context"
	"net"

	N "github.com/sagernet/sing/common/network"
)

// These interfaces intentionally mirror sing-box's adapter.V2RayServerTransport /
// V2RayClientTransport / V2RayServerTransportHandler. They are duplicated here
// (rather than imported) so that this library does NOT depend on sing-box.
// Concrete sing-box types satisfy them by structural typing.
//
// On the sing-box integration side, the consumer wires a thin adapter; see
// patches/0001-sing-box-register-xhttp-transport.patch for a working example.

// ServerTransport is the inbound side: a Server implements it.
type ServerTransport interface {
	Network() []string
	Serve(listener net.Listener) error
	ServePacket(listener net.PacketConn) error
	Close() error
}

// ClientTransport is the outbound side: a Client implements it.
type ClientTransport interface {
	DialContext(ctx context.Context) (net.Conn, error)
	Close() error
}

// ServerHandler is what the embedding application supplies to receive
// incoming TCP-style streams. The signature matches sing's
// N.TCPConnectionHandlerEx so a sing-box V2RayServerTransportHandler
// satisfies it as-is.
type ServerHandler interface {
	N.TCPConnectionHandlerEx
}
