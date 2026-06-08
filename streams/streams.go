// Package streams wraps the gRPC StreamService with idiomatic Go
// types and concurrency primitives.
//
// A [Client] is bound to a single store; callers working across
// multiple stores create one Client per store. Construction is
// stateless and cheap — Clients share the underlying *grpc.ClientConn.
//
// Usage:
//
//	c, _ := reckon.Connect(ctx, "beam01.lab:50051")
//	defer c.Close()
//	s := c.Streams("default_store")
//
//	// Append.
//	res, err := s.Append(ctx, "users-42", streams.AnyVersion, []streams.ProposedEvent{
//	    {EventType: "user_registered_v1", Data: payload},
//	})
//
//	// Read forward.
//	events, _ := s.Read(ctx, "users-42", 0, 100)
//
//	// Live tail.
//	ch, errs := s.Watch(ctx, "users-42", 0, 0)
//	for ev := range ch { handle(ev) }
//	if err := <-errs; err != nil { ... }
package streams

import (
	"context"
	"errors"
	"io"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
)

// ExpectedVersion drives optimistic concurrency on [Client.Append].
// Use one of the named constants or a non-negative version number for
// an exact-match check.
type ExpectedVersion int64

const (
	// NoStream — append fails unless the stream does not yet exist.
	NoStream ExpectedVersion = -1
	// AnyVersion — no concurrency check.
	AnyVersion ExpectedVersion = -2
	// StreamExists — append fails unless the stream already exists.
	StreamExists ExpectedVersion = -4
)

// TagMatch selects ANY-of vs ALL-of semantics for [Client.ReadByTags].
type TagMatch string

const (
	TagMatchAny TagMatch = "any"
	TagMatchAll TagMatch = "all"
)

func tagMatchToProto(m TagMatch) pb.TagMatch {
	if m == TagMatchAll {
		return pb.TagMatch_TAG_MATCH_ALL
	}
	return pb.TagMatch_TAG_MATCH_ANY
}

// ProposedEvent is an event to be appended. Zero-value fields mean
// "let the server decide" (server generates [ProposedEvent.EventID]
// if empty, and content-type fields default to "application/json").
type ProposedEvent struct {
	EventID             string
	EventType           string
	Data                []byte
	Metadata            []byte
	Tags                []string
	DataContentType     string
	MetadataContentType string
}

func (e ProposedEvent) toProto() *pb.ProposedEvent {
	return &pb.ProposedEvent{
		EventId:             e.EventID,
		EventType:           e.EventType,
		Data:                e.Data,
		Metadata:            e.Metadata,
		Tags:                e.Tags,
		DataContentType:     e.DataContentType,
		MetadataContentType: e.MetadataContentType,
	}
}

// RecordedEvent is a persisted event as read back from a stream.
type RecordedEvent struct {
	EventID             string
	EventType           string
	StreamID            string
	Version             uint64
	Data                []byte
	Metadata            []byte
	Tags                []string
	Timestamp           time.Time
	Epoch               time.Time
	DataContentType     string
	MetadataContentType string

	// PrevEventHash is the SHA-256 chain hash linking this event to
	// its predecessor in the same stream. Empty for legacy events
	// written before integrity was enabled for the stream. See the
	// proto definition for the verification model.
	PrevEventHash []byte
}

// RecordedEventFromProto converts a wire-shape *pb.RecordedEvent
// into the idiomatic Go [RecordedEvent]. Exposed for sibling
// packages (e.g. [codeberg.org/reckon-db-org/reckon-go/dcb]) that
// need to surface the same event type without re-deriving the
// field-by-field mapping.
func RecordedEventFromProto(p *pb.RecordedEvent) RecordedEvent {
	return recordedFromProto(p)
}

func recordedFromProto(p *pb.RecordedEvent) RecordedEvent {
	return RecordedEvent{
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

func recordedSliceFromProto(ps []*pb.RecordedEvent) []RecordedEvent {
	out := make([]RecordedEvent, 0, len(ps))
	for _, p := range ps {
		out = append(out, recordedFromProto(p))
	}
	return out
}

// AppendResult is what the server returned for a successful Append.
type AppendResult struct {
	// Version is the version of the last event written.
	Version uint64
	// Position is the global position of the last event written.
	Position uint64
	// Count is the number of events written.
	Count uint64
}

// Client is the typed wrapper for one store. Construct via [New].
type Client struct {
	raw   pb.StreamServiceClient
	store string
}

// New returns a Client that issues RPCs over conn, scoped to storeID.
func New(conn grpc.ClientConnInterface, storeID string) *Client {
	return &Client{raw: pb.NewStreamServiceClient(conn), store: storeID}
}

// StoreID returns the store this Client is bound to.
func (c *Client) StoreID() string { return c.store }

// Append writes events to streamID with the given concurrency check.
func (c *Client) Append(ctx context.Context, streamID string, expected ExpectedVersion, events []ProposedEvent, opts ...grpc.CallOption) (AppendResult, error) {
	pe := make([]*pb.ProposedEvent, 0, len(events))
	for _, e := range events {
		pe = append(pe, e.toProto())
	}
	resp, err := c.raw.AppendEvents(ctx, &pb.AppendEventsRequest{
		StoreId:         c.store,
		StreamId:        streamID,
		ExpectedVersion: int64(expected),
		Events:          pe,
	}, opts...)
	if err != nil {
		return AppendResult{}, err
	}
	return AppendResult{
		Version:  resp.GetVersion(),
		Position: resp.GetPosition(),
		Count:    resp.GetCount(),
	}, nil
}

// Read returns up to count events from streamID, starting at startVersion,
// in forward order.
func (c *Client) Read(ctx context.Context, streamID string, startVersion, count uint64, opts ...grpc.CallOption) ([]RecordedEvent, error) {
	resp, err := c.raw.ReadStreamForward(ctx, &pb.ReadStreamRequest{
		StoreId:      c.store,
		StreamId:     streamID,
		StartVersion: startVersion,
		Count:        count,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return recordedSliceFromProto(resp.GetEvents()), nil
}

// ReadBackward returns up to count events from streamID, ending at
// startVersion, in reverse order.
func (c *Client) ReadBackward(ctx context.Context, streamID string, startVersion, count uint64, opts ...grpc.CallOption) ([]RecordedEvent, error) {
	resp, err := c.raw.ReadStreamBackward(ctx, &pb.ReadStreamRequest{
		StoreId:      c.store,
		StreamId:     streamID,
		StartVersion: startVersion,
		Count:        count,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return recordedSliceFromProto(resp.GetEvents()), nil
}

// Watch subscribes to events appended to streamID, starting at
// startVersion. count is a max-events hint to the server; pass 0 for
// "no upper bound" (server-streaming continues until ctx is cancelled
// or the stream closes).
//
// Returns:
//   - events channel that closes when the stream ends
//   - errs channel that yields at most one error after events closes
//
// Backpressure: the receiver blocks if it doesn't drain events promptly.
func (c *Client) Watch(ctx context.Context, streamID string, startVersion, count uint64) (<-chan RecordedEvent, <-chan error) {
	events := make(chan RecordedEvent)
	errs := make(chan error, 1)

	go func() {
		defer close(events)
		defer close(errs)

		stream, err := c.raw.StreamEventsForward(ctx, &pb.ReadStreamRequest{
			StoreId:      c.store,
			StreamId:     streamID,
			StartVersion: startVersion,
			Count:        count,
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
			select {
			case events <- recordedFromProto(msg):
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
		}
	}()

	return events, errs
}

// Version returns the current version (last event number) of
// streamID. A return of -1 means the stream has no events.
func (c *Client) Version(ctx context.Context, streamID string, opts ...grpc.CallOption) (int64, error) {
	resp, err := c.raw.GetStreamVersion(ctx, &pb.GetStreamVersionRequest{
		StoreId:  c.store,
		StreamId: streamID,
	}, opts...)
	if err != nil {
		return 0, err
	}
	return resp.GetVersion(), nil
}

// List returns every stream_id present in the store.
func (c *Client) List(ctx context.Context, opts ...grpc.CallOption) ([]string, error) {
	resp, err := c.raw.ListStreams(ctx, &pb.ListStreamsRequest{StoreId: c.store}, opts...)
	if err != nil {
		return nil, err
	}
	return resp.GetStreamIds(), nil
}

// Delete removes streamID. Behaviour for non-existent streams is
// defined by the server (typically idempotent).
func (c *Client) Delete(ctx context.Context, streamID string, opts ...grpc.CallOption) error {
	_, err := c.raw.DeleteStream(ctx, &pb.DeleteStreamRequest{
		StoreId:  c.store,
		StreamId: streamID,
	}, opts...)
	return err
}

// ReadByEventTypes returns events of the given types across all
// streams in the store. batch is a server-side batch-size hint;
// pass 0 to let the server choose.
func (c *Client) ReadByEventTypes(ctx context.Context, types []string, batch uint64, opts ...grpc.CallOption) ([]RecordedEvent, error) {
	resp, err := c.raw.ReadByEventTypes(ctx, &pb.ReadByEventTypesRequest{
		StoreId:    c.store,
		EventTypes: types,
		BatchSize:  batch,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return recordedSliceFromProto(resp.GetEvents()), nil
}

// ReadByTags returns events that carry the given tags across all
// streams in the store. match selects ANY-of vs ALL-of semantics;
// batch is a server-side batch-size hint.
func (c *Client) ReadByTags(ctx context.Context, tags []string, match TagMatch, batch uint64, opts ...grpc.CallOption) ([]RecordedEvent, error) {
	resp, err := c.raw.ReadByTags(ctx, &pb.ReadByTagsRequest{
		StoreId:   c.store,
		Tags:      tags,
		Match:     tagMatchToProto(match),
		BatchSize: batch,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return recordedSliceFromProto(resp.GetEvents()), nil
}

// ReadByMetadata returns events whose metadata key == value across all
// streams in the store. batch is a server-side batch-size hint; pass 0 to
// let the server choose.
//
// This is the cross-cutting primitive for application-built
// causation/correlation read models (e.g. key "causation_id"). It is
// O(matches) when the store declared the {meta, key} secondary index,
// otherwise a server-side scan. The store does not interpret the key.
func (c *Client) ReadByMetadata(ctx context.Context, key, value string, batch uint64, opts ...grpc.CallOption) ([]RecordedEvent, error) {
	resp, err := c.raw.ReadByMetadata(ctx, &pb.ReadByMetadataRequest{
		StoreId:   c.store,
		Key:       key,
		Value:     value,
		BatchSize: batch,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return recordedSliceFromProto(resp.GetEvents()), nil
}

// ReadAllGlobal returns up to limit events from the store, ordered
// globally by epoch_us, starting at offset.
func (c *Client) ReadAllGlobal(ctx context.Context, offset, limit uint64, opts ...grpc.CallOption) ([]RecordedEvent, error) {
	resp, err := c.raw.ReadAllGlobal(ctx, &pb.ReadAllGlobalRequest{
		StoreId: c.store,
		Offset:  offset,
		Limit:   limit,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return recordedSliceFromProto(resp.GetEvents()), nil
}
