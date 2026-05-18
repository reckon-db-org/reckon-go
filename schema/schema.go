// Package schema wraps the gRPC SchemaService.
//
// Schemas are versioned per event type. The store rejects appends
// whose event type has a registered schema and whose payload fails
// validation against it.
//
// Upcast walks a slice of events through their registered upcasters
// (server-side) and returns the upgraded versions — useful when
// rehydrating an aggregate from a long-lived stream that spans
// multiple schema versions.
//
// Usage:
//
//	c, _ := reckon.Connect(ctx, "beam01.lab:50051")
//	defer c.Close()
//	s := c.Schema("default_store")
//
//	_ = s.Register(ctx, "user_registered_v1", schemaBlob)
//	got, _ := s.Get(ctx, "user_registered_v1")
//	all, _ := s.List(ctx)
package schema

import (
	"context"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"codeberg.org/reckon-db-org/reckon-go/streams"
	"google.golang.org/grpc"
)

// Definition is a registered schema as read back from the store.
type Definition struct {
	EventType string
	Schema    []byte
	Version   uint32
}

func definitionFromProto(p *pb.GetSchemaResponse) Definition {
	return Definition{
		EventType: p.GetEventType(),
		Schema:    p.GetSchema(),
		Version:   p.GetVersion(),
	}
}

// Client is the typed wrapper for one store. Construct via [New].
type Client struct {
	raw   pb.SchemaServiceClient
	store string
}

// New returns a Client that issues RPCs over conn, scoped to storeID.
func New(conn grpc.ClientConnInterface, storeID string) *Client {
	return &Client{raw: pb.NewSchemaServiceClient(conn), store: storeID}
}

// StoreID returns the store this Client is bound to.
func (c *Client) StoreID() string { return c.store }

// Register associates a schema blob with an event type. The blob is
// opaque to the gateway; the store reads it for validation.
func (c *Client) Register(ctx context.Context, eventType string, blob []byte, opts ...grpc.CallOption) error {
	_, err := c.raw.RegisterSchema(ctx, &pb.RegisterSchemaRequest{
		StoreId:   c.store,
		EventType: eventType,
		Schema:    blob,
	}, opts...)
	return err
}

// Unregister removes the schema for an event type.
func (c *Client) Unregister(ctx context.Context, eventType string, opts ...grpc.CallOption) error {
	_, err := c.raw.UnregisterSchema(ctx, &pb.UnregisterSchemaRequest{
		StoreId:   c.store,
		EventType: eventType,
	}, opts...)
	return err
}

// Get returns the registered schema for an event type. The returned
// Definition has Version == 0 and Schema == nil when no schema is
// registered.
func (c *Client) Get(ctx context.Context, eventType string, opts ...grpc.CallOption) (Definition, error) {
	resp, err := c.raw.GetSchema(ctx, &pb.GetSchemaRequest{
		StoreId:   c.store,
		EventType: eventType,
	}, opts...)
	if err != nil {
		return Definition{}, err
	}
	return definitionFromProto(resp), nil
}

// List returns every registered schema in the store.
func (c *Client) List(ctx context.Context, opts ...grpc.CallOption) ([]Definition, error) {
	resp, err := c.raw.ListSchemas(ctx, &pb.ListSchemasRequest{StoreId: c.store}, opts...)
	if err != nil {
		return nil, err
	}
	out := make([]Definition, 0, len(resp.GetSchemas()))
	for _, p := range resp.GetSchemas() {
		out = append(out, definitionFromProto(p))
	}
	return out, nil
}

// Version returns the current schema version for an event type.
// Returns 0 if no schema is registered.
func (c *Client) Version(ctx context.Context, eventType string, opts ...grpc.CallOption) (uint32, error) {
	resp, err := c.raw.GetSchemaVersion(ctx, &pb.GetSchemaVersionRequest{
		StoreId:   c.store,
		EventType: eventType,
	}, opts...)
	if err != nil {
		return 0, err
	}
	return resp.GetVersion(), nil
}

// Upcast walks each event through any registered upcasters and
// returns the upgraded versions. Events without an applicable
// upcaster are returned unchanged.
func (c *Client) Upcast(ctx context.Context, events []streams.RecordedEvent, opts ...grpc.CallOption) ([]streams.RecordedEvent, error) {
	in := make([]*pb.RecordedEvent, 0, len(events))
	for _, e := range events {
		in = append(in, toProto(e))
	}
	resp, err := c.raw.UpcastEvents(ctx, &pb.UpcastEventsRequest{
		StoreId: c.store,
		Events:  in,
	}, opts...)
	if err != nil {
		return nil, err
	}
	out := make([]streams.RecordedEvent, 0, len(resp.GetEvents()))
	for _, p := range resp.GetEvents() {
		out = append(out, fromProto(p))
	}
	return out, nil
}

func toProto(e streams.RecordedEvent) *pb.RecordedEvent {
	return &pb.RecordedEvent{
		EventId:             e.EventID,
		EventType:           e.EventType,
		StreamId:            e.StreamID,
		Version:             e.Version,
		Data:                e.Data,
		Metadata:            e.Metadata,
		Tags:                e.Tags,
		Timestamp:           e.Timestamp.UnixMilli(),
		EpochUs:             e.Epoch.UnixMicro(),
		DataContentType:     e.DataContentType,
		MetadataContentType: e.MetadataContentType,
		PrevEventHash:       e.PrevEventHash,
	}
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
