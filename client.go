package reckon

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
// Transport security: when no DialOptions are given, the connection uses
// TLS verified against the system root pool. Plaintext is an explicit
// opt-in via reckon.Insecure() — never a silent default (dialing a
// plaintext gateway over default TLS fails loudly with a handshake
// error). Custom trust anchors via reckon.TLSWithCA. Callers passing
// their own DialOptions own the transport credentials entirely, exactly
// as with grpc.NewClient.
func Connect(ctx context.Context, endpoint string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{defaultTLS()}
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

// Insecure returns a DialOption selecting plaintext, unauthenticated
// transport. Anyone on the network path can read and tamper with the
// traffic — lab and loopback use only.
func Insecure() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// TLSWithCA returns a DialOption that verifies the gateway against the
// PEM bundle at caFile instead of the system roots (self-signed or
// private-CA deployments). serverName overrides the hostname used for
// certificate verification (and SNI); pass "" to verify against the
// dialed host.
func TLSWithCA(caFile, serverName string) (grpc.DialOption, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates found in %s", caFile)
	}
	cfg := &tls.Config{
		RootCAs:    pool,
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(cfg)), nil
}

// TLSWithServerName returns a DialOption using the system root pool with
// an explicit verification/SNI hostname. Useful when dialing by IP while
// the gateway certificate carries a DNS name.
func TLSWithServerName(serverName string) grpc.DialOption {
	cfg := &tls.Config{
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(cfg))
}

func defaultTLS() grpc.DialOption {
	return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12,
	}))
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
