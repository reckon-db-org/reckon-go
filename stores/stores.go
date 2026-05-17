// Package stores wraps the gRPC StoresService with idiomatic Go
// types and concurrency primitives.
//
// Stores in ReckonDB are ephemeral — they exist when their
// supervision tree is running on at least one cluster node. There is
// no CreateStore / DeleteStore. Lifecycle is a deployment concern.
// This package exposes discovery only: who's currently announcing
// themselves to the cluster, and topology changes as they happen.
//
// Usage:
//
//	c, _ := reckon.Connect(ctx, "beam01.lab:50051")
//	defer c.Close()
//	sc := stores.New(c.Conn())
//
//	insts, err := sc.List(ctx)
//	for _, i := range insts {
//	    fmt.Printf("%s @ %s (%s)\n", i.StoreID, i.Node, i.Mode)
//	}
//
//	events, errs := sc.Watch(ctx, stores.WithSnapshot(true))
//	for ev := range events {
//	    fmt.Printf("%-9s %s @ %s\n", ev.Type, ev.Instance.StoreID, ev.Instance.Node)
//	}
//	if err := <-errs; err != nil { ... }
package stores

import (
	"context"
	"errors"
	"io"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
)

// Mode mirrors the wire enum but presents as a string in Go.
type Mode string

const (
	ModeUnspecified Mode = "unspecified"
	ModeSingle      Mode = "single"
	ModeCluster     Mode = "cluster"
)

func modeFromProto(m pb.StoreMode) Mode {
	switch m {
	case pb.StoreMode_STORE_MODE_SINGLE:
		return ModeSingle
	case pb.StoreMode_STORE_MODE_CLUSTER:
		return ModeCluster
	default:
		return ModeUnspecified
	}
}

// EventType — store appeared on a node, or retired.
type EventType string

const (
	EventAnnounced EventType = "announced"
	EventRetired   EventType = "retired"
)

func eventTypeFromProto(t pb.StoreEventType) EventType {
	switch t {
	case pb.StoreEventType_STORE_EVENT_TYPE_RETIRED:
		return EventRetired
	default:
		return EventAnnounced
	}
}

// Instance is one (store_id, node) registration as observed by the
// gateway's discovery layer. A store running on N nodes yields N
// Instances; clients that want a single logical view group by
// StoreID themselves.
type Instance struct {
	StoreID      string
	Node         string
	Mode         Mode
	DataDir      string
	Timeout      time.Duration
	RegisteredAt time.Time
}

func instanceFromProto(p *pb.StoreInstance) Instance {
	return Instance{
		StoreID:      p.GetStoreId(),
		Node:         p.GetNode(),
		Mode:         modeFromProto(p.GetMode()),
		DataDir:      p.GetDataDir(),
		Timeout:      time.Duration(p.GetTimeoutMs()) * time.Millisecond,
		RegisteredAt: time.UnixMicro(p.GetRegisteredAtUs()),
	}
}

// Event is one entry from a Watch stream.
type Event struct {
	Type     EventType
	Instance Instance
	At       time.Time
}

// Client is the typed wrapper. Construct via [New].
type Client struct {
	raw pb.StoresServiceClient
}

// New returns a Client that issues RPCs over conn.
func New(conn grpc.ClientConnInterface) *Client {
	return &Client{raw: pb.NewStoresServiceClient(conn)}
}

// List returns every (store_id, node) currently announced in the cluster.
func (c *Client) List(ctx context.Context, opts ...grpc.CallOption) ([]Instance, error) {
	resp, err := c.raw.ListStores(ctx, &pb.ListStoresRequest{}, opts...)
	if err != nil {
		return nil, err
	}
	out := make([]Instance, 0, len(resp.GetInstances()))
	for _, p := range resp.GetInstances() {
		out = append(out, instanceFromProto(p))
	}
	return out, nil
}

// Get returns the Instances for a specific store_id — one per node
// currently hosting it. Returns an empty slice (not an error) if no
// node is announcing this store.
func (c *Client) Get(ctx context.Context, storeID string, opts ...grpc.CallOption) ([]Instance, error) {
	resp, err := c.raw.GetStore(ctx, &pb.GetStoreRequest{StoreId: storeID}, opts...)
	if err != nil {
		return nil, err
	}
	out := make([]Instance, 0, len(resp.GetInstances()))
	for _, p := range resp.GetInstances() {
		out = append(out, instanceFromProto(p))
	}
	return out, nil
}

// WatchOption customises [Client.Watch].
type WatchOption func(*watchOpts)

type watchOpts struct {
	snapshot bool
}

// WithSnapshot — when true (default), Watch emits one [EventAnnounced]
// for each currently-known instance before going live.
func WithSnapshot(snapshot bool) WatchOption {
	return func(o *watchOpts) { o.snapshot = snapshot }
}

// Watch subscribes to cluster topology changes. Returns:
//
//   - an events channel that closes when the stream ends
//   - an errs channel that yields exactly one error (or nil) after
//     events closes, suitable for a single read after the events range
//
// Cancel via ctx. Backpressure: receiver blocks if it doesn't drain
// events promptly.
func (c *Client) Watch(ctx context.Context, options ...WatchOption) (<-chan Event, <-chan error) {
	opts := watchOpts{snapshot: true}
	for _, o := range options {
		o(&opts)
	}

	events := make(chan Event)
	errs := make(chan error, 1)

	go func() {
		defer close(events)
		defer close(errs)

		stream, err := c.raw.WatchStores(ctx, &pb.WatchStoresRequest{
			IncludeSnapshot: opts.snapshot,
		})
		if err != nil {
			errs <- err
			return
		}
		for {
			msg, err := stream.Recv()
			switch {
			case errors.Is(err, io.EOF):
				return
			case err != nil:
				errs <- err
				return
			}
			ev := Event{
				Type:     eventTypeFromProto(msg.GetType()),
				Instance: instanceFromProto(msg.GetInstance()),
				At:       time.UnixMicro(msg.GetEventAtUs()),
			}
			select {
			case events <- ev:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
		}
	}()

	return events, errs
}
