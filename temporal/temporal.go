// Package temporal wraps the gRPC TemporalService for time-based
// stream reads.
//
// Three queries:
//   - [Client.Until] — every event in a stream up to a wall-clock
//     timestamp, ordered oldest-first.
//   - [Client.Range] — every event in a stream between two
//     timestamps (inclusive), ordered oldest-first.
//   - [Client.VersionAt] — the event version that was current at a
//     given timestamp (no events returned).
//
// Timestamps are Go [time.Time]; the wire layer uses Unix epoch
// milliseconds.
//
// Usage:
//
//	c, _ := reckon.Connect(ctx, "beam01.lab:50051")
//	defer c.Close()
//	t := c.Temporal("default_store")
//
//	cutoff := time.Now().Add(-1 * time.Hour)
//	evts, _ := t.Until(ctx, "users-42", cutoff, 1000)
package temporal

import (
	"context"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"codeberg.org/reckon-db-org/reckon-go/streams"
	"google.golang.org/grpc"
)

// Client is the typed wrapper for one store. Construct via [New].
type Client struct {
	raw   pb.TemporalServiceClient
	store string
}

// New returns a Client that issues RPCs over conn, scoped to storeID.
func New(conn grpc.ClientConnInterface, storeID string) *Client {
	return &Client{raw: pb.NewTemporalServiceClient(conn), store: storeID}
}

// StoreID returns the store this Client is bound to.
func (c *Client) StoreID() string { return c.store }

// Until returns every event in streamID with timestamp <= cutoff,
// oldest first, up to batch entries.
func (c *Client) Until(ctx context.Context, streamID string, cutoff time.Time, batch uint64, opts ...grpc.CallOption) ([]streams.RecordedEvent, error) {
	resp, err := c.raw.ReadUntil(ctx, &pb.ReadUntilRequest{
		StoreId:   c.store,
		StreamId:  streamID,
		Timestamp: cutoff.UnixMilli(),
		BatchSize: batch,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return fromProtoSlice(resp.GetEvents()), nil
}

// Range returns every event in streamID with timestamp in
// [from, to] (inclusive), oldest first, up to batch entries.
func (c *Client) Range(ctx context.Context, streamID string, from, to time.Time, batch uint64, opts ...grpc.CallOption) ([]streams.RecordedEvent, error) {
	resp, err := c.raw.ReadRange(ctx, &pb.ReadRangeRequest{
		StoreId:       c.store,
		StreamId:      streamID,
		FromTimestamp: from.UnixMilli(),
		ToTimestamp:   to.UnixMilli(),
		BatchSize:     batch,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return fromProtoSlice(resp.GetEvents()), nil
}

// VersionAt returns the version current at timestamp, or -1 if the
// stream had no events before timestamp.
func (c *Client) VersionAt(ctx context.Context, streamID string, at time.Time, opts ...grpc.CallOption) (int64, error) {
	resp, err := c.raw.VersionAt(ctx, &pb.VersionAtRequest{
		StoreId:   c.store,
		StreamId:  streamID,
		Timestamp: at.UnixMilli(),
	}, opts...)
	if err != nil {
		return 0, err
	}
	return resp.GetVersion(), nil
}

func fromProto(p *pb.RecordedEvent) streams.RecordedEvent {
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

func fromProtoSlice(ps []*pb.RecordedEvent) []streams.RecordedEvent {
	out := make([]streams.RecordedEvent, 0, len(ps))
	for _, p := range ps {
		out = append(out, fromProto(p))
	}
	return out
}
