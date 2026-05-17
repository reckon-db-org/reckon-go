package reckon

import (
	"context"

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
// Currently uses insecure transport. TLS + capability-token auth are
// follow-ups, tracked alongside the gateway's auth surface.
func Connect(ctx context.Context, endpoint string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}
	conn, err := grpc.NewClient(endpoint, opts...)
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
