package reckon

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the top-level facade. Open one per logical gateway endpoint;
// per-service sub-clients are accessed through the methods on Client.
type Client struct {
	conn *grpc.ClientConn
}

// Connect dials the given gateway endpoint and returns a ready Client.
// The endpoint is in `host:port` form (e.g. "beam01.lab:50051").
//
// A bare host:port target is dialed through the passthrough resolver, which
// delegates name resolution to the dialer (honouring /etc/hosts and nsswitch)
// and skips the default dns resolver's SRV/TXT service-config lookups. Those
// lookups hang for several seconds — or indefinitely — on private TLDs such
// as `*.lab`, whose nameservers don't answer the synthetic `_grpc_config.`
// and `_grpclb._tcp.` queries. Callers needing client-side load balancing or
// active re-resolution may pass an explicit scheme (e.g. "dns:///host:port")
// to override.
//
// Currently uses insecure transport. TLS + capability-token auth are
// follow-ups, tracked alongside the gateway's auth surface.
func Connect(ctx context.Context, endpoint string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}
	target := endpoint
	if !strings.Contains(endpoint, "://") {
		target = "passthrough:///" + endpoint
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// Close releases the underlying gRPC connection. Idempotent.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Conn returns the underlying *grpc.ClientConn. Useful when callers
// need to construct service stubs by hand (e.g. for an RPC not yet
// wrapped here, or to inject interceptors). Subject to change once
// the per-service Sub-Client API stabilises.
func (c *Client) Conn() *grpc.ClientConn { return c.conn }
