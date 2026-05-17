// Package subscriptions wraps the gRPC SubscriptionService with
// idiomatic Go types and concurrency primitives.
//
// A [Client] is bound to a single store. Subscriptions are persistent
// server-side checkpoints; the client receives events via a streaming
// RPC and advances the checkpoint by [Client.Ack]ing each one.
//
// Usage:
//
//	c, _ := reckon.Connect(ctx, "beam01.lab:50051")
//	defer c.Close()
//	subs := c.Subscriptions("default_store")
//
//	// Create a persistent subscription up-front (optional — Subscribe
//	// creates one too).
//	_, _ = subs.Create(ctx, subscriptions.Spec{
//	    Type:     subscriptions.TypeStream,
//	    Selector: "users-42",
//	    Name:     "billing-projection",
//	})
//
//	ch, errs := subs.Subscribe(ctx, subscriptions.Spec{
//	    Type:     subscriptions.TypeStream,
//	    Selector: "users-42",
//	    Name:     "billing-projection",
//	})
//	for d := range ch {
//	    process(d.Event)
//	    _ = subs.Ack(ctx, d.Event.StreamID, "billing-projection", d.Event.Version)
//	}
//	if err := <-errs; err != nil { ... }
package subscriptions

import (
	"context"
	"errors"
	"io"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"codeberg.org/reckon-db-org/reckon-go/streams"
	"google.golang.org/grpc"
)

// Type categorises a subscription's selector. See the proto for the
// exact selector grammar per type.
type Type string

const (
	TypeUnspecified  Type = "unspecified"
	TypeStream       Type = "stream"
	TypeEventType    Type = "event_type"
	TypeEventPattern Type = "event_pattern"
	TypeEventPayload Type = "event_payload"
	TypeTags         Type = "tags"
)

func typeToProto(t Type) pb.SubscriptionType {
	switch t {
	case TypeStream:
		return pb.SubscriptionType_SUBSCRIPTION_TYPE_STREAM
	case TypeEventType:
		return pb.SubscriptionType_SUBSCRIPTION_TYPE_EVENT_TYPE
	case TypeEventPattern:
		return pb.SubscriptionType_SUBSCRIPTION_TYPE_EVENT_PATTERN
	case TypeEventPayload:
		return pb.SubscriptionType_SUBSCRIPTION_TYPE_EVENT_PAYLOAD
	case TypeTags:
		return pb.SubscriptionType_SUBSCRIPTION_TYPE_TAGS
	default:
		return pb.SubscriptionType_SUBSCRIPTION_TYPE_UNSPECIFIED
	}
}

func typeFromProto(t pb.SubscriptionType) Type {
	switch t {
	case pb.SubscriptionType_SUBSCRIPTION_TYPE_STREAM:
		return TypeStream
	case pb.SubscriptionType_SUBSCRIPTION_TYPE_EVENT_TYPE:
		return TypeEventType
	case pb.SubscriptionType_SUBSCRIPTION_TYPE_EVENT_PATTERN:
		return TypeEventPattern
	case pb.SubscriptionType_SUBSCRIPTION_TYPE_EVENT_PAYLOAD:
		return TypeEventPayload
	case pb.SubscriptionType_SUBSCRIPTION_TYPE_TAGS:
		return TypeTags
	default:
		return TypeUnspecified
	}
}

// Spec describes a subscription target. Used by both [Client.Create]
// and [Client.Subscribe].
type Spec struct {
	// Type categorises Selector.
	Type Type
	// Selector — stream id, event-type name, pattern, etc. (depends
	// on Type).
	Selector string
	// Name uniquely identifies the persistent subscription within the
	// store; reused across reconnects.
	Name string
	// StartFrom is the event number to begin from on first creation.
	// Ignored once the subscription exists (server resumes from its
	// stored checkpoint).
	StartFrom uint64
	// PoolSize sizes the server-side delivery worker pool. 0 = server
	// default.
	PoolSize uint32
}

// Info describes a persistent subscription's current state.
type Info struct {
	ID         string
	Type       Type
	Selector   string
	Name       string
	CreatedAt  time.Time
	PoolSize   uint32
	Checkpoint uint64
}

func infoFromProto(p *pb.SubscriptionInfo) Info {
	return Info{
		ID:         p.GetId(),
		Type:       typeFromProto(p.GetType()),
		Selector:   p.GetSelector(),
		Name:       p.GetSubscriptionName(),
		CreatedAt:  time.UnixMilli(p.GetCreatedAt()),
		PoolSize:   p.GetPoolSize(),
		Checkpoint: p.GetCheckpoint(),
	}
}

// Delivery is one entry on a [Client.Subscribe] stream: the event
// itself plus the server-side checkpoint after delivery.
type Delivery struct {
	Event      streams.RecordedEvent
	Checkpoint uint64
}

// Lag reports how far behind a subscription is.
type Lag struct {
	Lag               uint64
	CurrentCheckpoint uint64
	LatestVersion     uint64
}

// Client is the typed wrapper for one store. Construct via [New].
type Client struct {
	raw   pb.SubscriptionServiceClient
	store string
}

// New returns a Client that issues RPCs over conn, scoped to storeID.
func New(conn grpc.ClientConnInterface, storeID string) *Client {
	return &Client{raw: pb.NewSubscriptionServiceClient(conn), store: storeID}
}

// StoreID returns the store this Client is bound to.
func (c *Client) StoreID() string { return c.store }

// Create registers a persistent subscription without opening a
// delivery stream. Useful when one process provisions subscriptions
// for others to consume. Returns the server-assigned subscription id.
func (c *Client) Create(ctx context.Context, spec Spec, opts ...grpc.CallOption) (string, error) {
	resp, err := c.raw.CreateSubscription(ctx, &pb.CreateSubscriptionRequest{
		StoreId:          c.store,
		Type:             typeToProto(spec.Type),
		Selector:         spec.Selector,
		SubscriptionName: spec.Name,
		StartFrom:        spec.StartFrom,
		PoolSize:         spec.PoolSize,
	}, opts...)
	if err != nil {
		return "", err
	}
	return resp.GetSubscriptionId(), nil
}

// Remove deletes a persistent subscription. spec.Type and spec.Selector
// must match the original; spec.StartFrom and spec.PoolSize are ignored.
func (c *Client) Remove(ctx context.Context, spec Spec, opts ...grpc.CallOption) error {
	_, err := c.raw.RemoveSubscription(ctx, &pb.RemoveSubscriptionRequest{
		StoreId:          c.store,
		Type:             typeToProto(spec.Type),
		Selector:         spec.Selector,
		SubscriptionName: spec.Name,
	}, opts...)
	return err
}

// List returns every persistent subscription in the store.
func (c *Client) List(ctx context.Context, opts ...grpc.CallOption) ([]Info, error) {
	resp, err := c.raw.ListSubscriptions(ctx, &pb.ListSubscriptionsRequest{StoreId: c.store}, opts...)
	if err != nil {
		return nil, err
	}
	out := make([]Info, 0, len(resp.GetSubscriptions()))
	for _, p := range resp.GetSubscriptions() {
		out = append(out, infoFromProto(p))
	}
	return out, nil
}

// Get returns a single subscription's Info by name.
func (c *Client) Get(ctx context.Context, name string, opts ...grpc.CallOption) (Info, error) {
	resp, err := c.raw.GetSubscription(ctx, &pb.GetSubscriptionRequest{
		StoreId:          c.store,
		SubscriptionName: name,
	}, opts...)
	if err != nil {
		return Info{}, err
	}
	return infoFromProto(resp), nil
}

// Lag returns how many events the subscription has yet to deliver.
func (c *Client) Lag(ctx context.Context, name string, opts ...grpc.CallOption) (Lag, error) {
	resp, err := c.raw.GetSubscriptionLag(ctx, &pb.GetSubscriptionLagRequest{
		StoreId:          c.store,
		SubscriptionName: name,
	}, opts...)
	if err != nil {
		return Lag{}, err
	}
	return Lag{
		Lag:               resp.GetLag(),
		CurrentCheckpoint: resp.GetCurrentCheckpoint(),
		LatestVersion:     resp.GetLatestVersion(),
	}, nil
}

// Ack advances the subscription's checkpoint past eventNumber on
// streamID. Required for at-least-once delivery semantics.
func (c *Client) Ack(ctx context.Context, streamID, name string, eventNumber uint64, opts ...grpc.CallOption) error {
	_, err := c.raw.AckEvent(ctx, &pb.AckEventRequest{
		StoreId:          c.store,
		StreamId:         streamID,
		SubscriptionName: name,
		EventNumber:      eventNumber,
	}, opts...)
	return err
}

// Subscribe opens a delivery stream. The subscription is created on
// the fly if it does not yet exist; otherwise the server resumes from
// the stored checkpoint (spec.StartFrom is ignored in that case).
//
// Returns:
//   - deliveries channel that closes when the stream ends
//   - errs channel that yields at most one error after deliveries closes
//
// The caller MUST call [Client.Ack] on each Delivery for the checkpoint
// to advance; failing to ack does not stop delivery but loses progress
// on reconnect.
//
// Backpressure: the receiver blocks if it doesn't drain deliveries
// promptly.
func (c *Client) Subscribe(ctx context.Context, spec Spec) (<-chan Delivery, <-chan error) {
	deliveries := make(chan Delivery)
	errs := make(chan error, 1)

	go func() {
		defer close(deliveries)
		defer close(errs)

		stream, err := c.raw.Subscribe(ctx, &pb.SubscribeRequest{
			StoreId:          c.store,
			Type:             typeToProto(spec.Type),
			Selector:         spec.Selector,
			SubscriptionName: spec.Name,
			StartFrom:        spec.StartFrom,
			PoolSize:         spec.PoolSize,
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
			d := Delivery{
				Event:      recordedFromProto(msg.GetEvent()),
				Checkpoint: msg.GetCheckpoint(),
			}
			select {
			case deliveries <- d:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
		}
	}()

	return deliveries, errs
}

// recordedFromProto bridges to streams.RecordedEvent without importing
// the proto types into the public surface.
func recordedFromProto(p *pb.RecordedEvent) streams.RecordedEvent {
	return streams.RecordedEvent{
		EventID:             p.GetEventId(),
		EventType:           p.GetEventType(),
		StreamID:            p.GetStreamId(),
		Version:             p.GetVersion(),
		Data:                p.GetData(),
		Metadata:            p.GetMetadata(),
		Tags:                p.GetTags(),
		Timestamp:           time.UnixMilli(p.GetTimestamp()),
		Epoch:               time.UnixMicro(p.GetEpochUs()),
		DataContentType:     p.GetDataContentType(),
		MetadataContentType: p.GetMetadataContentType(),
		PrevEventHash:       p.GetPrevEventHash(),
	}
}
