// Package causation wraps the gRPC CausationService for traversing
// event causation graphs.
//
// Events can carry two kinds of lineage references in their metadata:
//
//   - causation_id — the event that directly produced this one.
//   - correlation_id — a shared id binding events that participated
//     in the same logical operation (request, saga, etc).
//
// Causation forms a tree; correlation forms an equivalence class.
//
// Usage:
//
//	c, _ := reckon.Connect(ctx, "beam01.lab:50051")
//	defer c.Close()
//	cs := c.Causation("default_store")
//
//	effects, _ := cs.Effects(ctx, "evt-7c4...")    // direct children
//	chain, _ := cs.Chain(ctx, "evt-7c4...")        // full upstream chain
//	graph, _ := cs.Graph(ctx, "evt-7c4...")        // full downstream tree
package causation

import (
	"context"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"codeberg.org/reckon-db-org/reckon-go/streams"
	"google.golang.org/grpc"
)

// GraphNode is one node in a causation tree returned by
// [Client.Graph]. Children are the events caused by Event.
type GraphNode struct {
	Event    streams.RecordedEvent
	Children []GraphNode
}

// Client is the typed wrapper for one store. Construct via [New].
type Client struct {
	raw   pb.CausationServiceClient
	store string
}

// New returns a Client that issues RPCs over conn, scoped to storeID.
func New(conn grpc.ClientConnInterface, storeID string) *Client {
	return &Client{raw: pb.NewCausationServiceClient(conn), store: storeID}
}

// StoreID returns the store this Client is bound to.
func (c *Client) StoreID() string { return c.store }

// Effects returns the events directly caused by eventID — the direct
// children in the causation tree.
func (c *Client) Effects(ctx context.Context, eventID string, opts ...grpc.CallOption) ([]streams.RecordedEvent, error) {
	resp, err := c.raw.GetEffects(ctx, &pb.CausationRequest{
		StoreId: c.store,
		EventId: eventID,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return fromProtoSlice(resp.GetEvents()), nil
}

// Cause returns the single event that directly caused eventID.
// Returns a zero RecordedEvent (EventID == "") when eventID has no
// recorded cause.
func (c *Client) Cause(ctx context.Context, eventID string, opts ...grpc.CallOption) (streams.RecordedEvent, error) {
	resp, err := c.raw.GetCause(ctx, &pb.CausationRequest{
		StoreId: c.store,
		EventId: eventID,
	}, opts...)
	if err != nil {
		return streams.RecordedEvent{}, err
	}
	if resp.GetEvent() == nil {
		return streams.RecordedEvent{}, nil
	}
	return fromProto(resp.GetEvent()), nil
}

// Chain returns the full upstream causation chain ending at eventID,
// ordered root-first.
func (c *Client) Chain(ctx context.Context, eventID string, opts ...grpc.CallOption) ([]streams.RecordedEvent, error) {
	resp, err := c.raw.GetCausationChain(ctx, &pb.CausationRequest{
		StoreId: c.store,
		EventId: eventID,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return fromProtoSlice(resp.GetEvents()), nil
}

// Correlated returns every event sharing correlationID, regardless
// of causal order.
func (c *Client) Correlated(ctx context.Context, correlationID string, opts ...grpc.CallOption) ([]streams.RecordedEvent, error) {
	resp, err := c.raw.GetCorrelated(ctx, &pb.CorrelationRequest{
		StoreId:       c.store,
		CorrelationId: correlationID,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return fromProtoSlice(resp.GetEvents()), nil
}

// Graph returns the full downstream causation tree rooted at eventID.
func (c *Client) Graph(ctx context.Context, eventID string, opts ...grpc.CallOption) (GraphNode, error) {
	resp, err := c.raw.BuildCausationGraph(ctx, &pb.CausationRequest{
		StoreId: c.store,
		EventId: eventID,
	}, opts...)
	if err != nil {
		return GraphNode{}, err
	}
	return nodeFromProto(resp.GetRoot()), nil
}

func nodeFromProto(p *pb.CausationGraphNode) GraphNode {
	if p == nil {
		return GraphNode{}
	}
	kids := make([]GraphNode, 0, len(p.GetChildren()))
	for _, k := range p.GetChildren() {
		kids = append(kids, nodeFromProto(k))
	}
	return GraphNode{
		Event:    fromProto(p.GetEvent()),
		Children: kids,
	}
}

func fromProto(p *pb.RecordedEvent) streams.RecordedEvent {
	if p == nil {
		return streams.RecordedEvent{}
	}
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
