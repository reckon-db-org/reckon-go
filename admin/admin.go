// Package admin wraps the gRPC AdminService.
//
// Three concerns under one service:
//
//   - Statistics: store, stream, event-type rollups.
//   - Scavenging: GC events from a stream, by stream id or pattern,
//     with a dry-run mode for capacity planning.
//   - Links: server-side projection/link lifecycle (Greg-Young style
//     `$link:` system streams that materialise filtered views).
//
// Usage:
//
//	c, _ := reckon.Connect(ctx, "beam01.lab:50051")
//	defer c.Close()
//	a := c.Admin("default_store")
//
//	stats, _ := a.StoreStats(ctx)
//	preview, _ := a.ScavengeDryRun(ctx, "tenant-acme", nil)
//	_ = a.CreateLink(ctx, "by-user", "$all", "by_user", nil)
//	_ = a.StartLink(ctx, "by-user")
package admin

import (
	"context"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
)

// StoreStats is the store-level rollup.
type StoreStats struct {
	TotalStreams       uint64
	TotalEvents        uint64
	TotalSubscriptions uint64
	TotalSnapshots     uint64
	Details            map[string]string
}

// StreamInfo is the per-stream rollup.
type StreamInfo struct {
	StreamID    string
	Version     uint64
	EventCount  uint64
	CreatedAt   time.Time
	LastEventAt time.Time
	EventTypes  []string
}

// EventTypeCount pairs an event type with its occurrence count.
type EventTypeCount struct {
	EventType string
	Count     uint64
}

// ScavengeResult is the outcome of a scavenge pass (real or dry-run).
type ScavengeResult struct {
	EventsRemoved       uint64
	EventsRemaining     uint64
	SpaceReclaimedBytes uint64
	Details             map[string]string
}

// LinkSpec defines a projection link. Source and Target are stream
// names; Options is implementation-dependent metadata.
type LinkSpec struct {
	Name    string
	Source  string
	Target  string
	Options map[string]string
}

// LinkRuntime is the operational state of a link.
type LinkRuntime struct {
	Name            string
	Status          string
	EventsProcessed uint64
	Details         map[string]string
}

// Client is the typed wrapper for one store. Construct via [New].
type Client struct {
	raw   pb.AdminServiceClient
	store string
}

// New returns a Client that issues RPCs over conn, scoped to storeID.
func New(conn grpc.ClientConnInterface, storeID string) *Client {
	return &Client{raw: pb.NewAdminServiceClient(conn), store: storeID}
}

// StoreID returns the store this Client is bound to.
func (c *Client) StoreID() string { return c.store }

// ---- Stats / info --------------------------------------------------

// StoreStats returns counts for streams, events, subscriptions, snapshots.
func (c *Client) StoreStats(ctx context.Context, opts ...grpc.CallOption) (StoreStats, error) {
	resp, err := c.raw.GetStoreStats(ctx, &pb.StoreStatsRequest{StoreId: c.store}, opts...)
	if err != nil {
		return StoreStats{}, err
	}
	return StoreStats{
		TotalStreams:       resp.GetTotalStreams(),
		TotalEvents:        resp.GetTotalEvents(),
		TotalSubscriptions: resp.GetTotalSubscriptions(),
		TotalSnapshots:     resp.GetTotalSnapshots(),
		Details:            resp.GetDetails(),
	}, nil
}

// StreamInfo returns the per-stream rollup.
func (c *Client) StreamInfo(ctx context.Context, streamID string, opts ...grpc.CallOption) (StreamInfo, error) {
	resp, err := c.raw.GetStreamInfo(ctx, &pb.StreamInfoRequest{
		StoreId:  c.store,
		StreamId: streamID,
	}, opts...)
	if err != nil {
		return StreamInfo{}, err
	}
	return StreamInfo{
		StreamID:    resp.GetStreamId(),
		Version:     resp.GetVersion(),
		EventCount:  resp.GetEventCount(),
		CreatedAt:   time.UnixMilli(resp.GetCreatedAt()),
		LastEventAt: time.UnixMilli(resp.GetLastEventAt()),
		EventTypes:  resp.GetEventTypes(),
	}, nil
}

// EventTypeSummary returns counts grouped by event type across the
// whole store.
func (c *Client) EventTypeSummary(ctx context.Context, opts ...grpc.CallOption) ([]EventTypeCount, error) {
	resp, err := c.raw.GetEventTypeSummary(ctx, &pb.EventTypeSummaryRequest{StoreId: c.store}, opts...)
	if err != nil {
		return nil, err
	}
	out := make([]EventTypeCount, 0, len(resp.GetEntries()))
	for _, e := range resp.GetEntries() {
		out = append(out, EventTypeCount{EventType: e.GetEventType(), Count: e.GetCount()})
	}
	return out, nil
}

// ---- Scavenge ------------------------------------------------------

// Scavenge GCs the streamID. options is forwarded as-is to the
// server-side scavenger (implementation-specific).
func (c *Client) Scavenge(ctx context.Context, streamID string, options map[string]string, opts ...grpc.CallOption) (ScavengeResult, error) {
	return c.scavenge(ctx, c.raw.Scavenge, streamID, options, opts)
}

// ScavengeDryRun returns what Scavenge would have removed, without
// touching the store.
func (c *Client) ScavengeDryRun(ctx context.Context, streamID string, options map[string]string, opts ...grpc.CallOption) (ScavengeResult, error) {
	return c.scavenge(ctx, c.raw.ScavengeDryRun, streamID, options, opts)
}

type scavengeFn func(context.Context, *pb.ScavengeRequest, ...grpc.CallOption) (*pb.ScavengeResponse, error)

func (c *Client) scavenge(ctx context.Context, fn scavengeFn, streamID string, options map[string]string, opts []grpc.CallOption) (ScavengeResult, error) {
	resp, err := fn(ctx, &pb.ScavengeRequest{
		StoreId:  c.store,
		StreamId: streamID,
		Options:  options,
	}, opts...)
	if err != nil {
		return ScavengeResult{}, err
	}
	return scavengeResultFromProto(resp), nil
}

// ScavengeMatching GCs every stream whose id matches pattern.
// Returns one result per matched stream.
func (c *Client) ScavengeMatching(ctx context.Context, pattern string, options map[string]string, opts ...grpc.CallOption) ([]ScavengeResult, error) {
	resp, err := c.raw.ScavengeMatching(ctx, &pb.ScavengeMatchingRequest{
		StoreId: c.store,
		Pattern: pattern,
		Options: options,
	}, opts...)
	if err != nil {
		return nil, err
	}
	out := make([]ScavengeResult, 0, len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		out = append(out, scavengeResultFromProto(r))
	}
	return out, nil
}

func scavengeResultFromProto(p *pb.ScavengeResponse) ScavengeResult {
	return ScavengeResult{
		EventsRemoved:       p.GetEventsRemoved(),
		EventsRemaining:     p.GetEventsRemaining(),
		SpaceReclaimedBytes: p.GetSpaceReclaimedBytes(),
		Details:             p.GetDetails(),
	}
}

// ---- Links ---------------------------------------------------------

// CreateLink registers a projection link (does not start it).
func (c *Client) CreateLink(ctx context.Context, spec LinkSpec, opts ...grpc.CallOption) error {
	_, err := c.raw.CreateLink(ctx, &pb.CreateLinkRequest{
		StoreId: c.store,
		Name:    spec.Name,
		Source:  spec.Source,
		Target:  spec.Target,
		Options: spec.Options,
	}, opts...)
	return err
}

// DeleteLink removes a link.
func (c *Client) DeleteLink(ctx context.Context, name string, opts ...grpc.CallOption) error {
	_, err := c.raw.DeleteLink(ctx, &pb.DeleteLinkRequest{
		StoreId: c.store,
		Name:    name,
	}, opts...)
	return err
}

// GetLink returns a link's static definition. Returns a zero LinkSpec
// (Name == "") when the link does not exist.
func (c *Client) GetLink(ctx context.Context, name string, opts ...grpc.CallOption) (LinkSpec, error) {
	resp, err := c.raw.GetLink(ctx, &pb.GetLinkRequest{
		StoreId: c.store,
		Name:    name,
	}, opts...)
	if err != nil {
		return LinkSpec{}, err
	}
	return LinkSpec{
		Name:    resp.GetName(),
		Source:  resp.GetSource(),
		Target:  resp.GetTarget(),
		Options: resp.GetOptions(),
	}, nil
}

// ListLinks returns every link in the store.
func (c *Client) ListLinks(ctx context.Context, opts ...grpc.CallOption) ([]LinkSpec, error) {
	resp, err := c.raw.ListLinks(ctx, &pb.ListLinksRequest{StoreId: c.store}, opts...)
	if err != nil {
		return nil, err
	}
	out := make([]LinkSpec, 0, len(resp.GetLinks()))
	for _, l := range resp.GetLinks() {
		out = append(out, LinkSpec{
			Name:    l.GetName(),
			Source:  l.GetSource(),
			Target:  l.GetTarget(),
			Options: l.GetOptions(),
		})
	}
	return out, nil
}

// StartLink turns on a link's projection.
func (c *Client) StartLink(ctx context.Context, name string, opts ...grpc.CallOption) error {
	_, err := c.raw.StartLink(ctx, &pb.StartLinkRequest{StoreId: c.store, Name: name}, opts...)
	return err
}

// StopLink pauses a link's projection (without deleting it).
func (c *Client) StopLink(ctx context.Context, name string, opts ...grpc.CallOption) error {
	_, err := c.raw.StopLink(ctx, &pb.StopLinkRequest{StoreId: c.store, Name: name}, opts...)
	return err
}

// LinkInfo returns runtime status for a link.
func (c *Client) LinkInfo(ctx context.Context, name string, opts ...grpc.CallOption) (LinkRuntime, error) {
	resp, err := c.raw.GetLinkInfo(ctx, &pb.GetLinkRequest{
		StoreId: c.store,
		Name:    name,
	}, opts...)
	if err != nil {
		return LinkRuntime{}, err
	}
	return LinkRuntime{
		Name:            resp.GetName(),
		Status:          resp.GetStatus(),
		EventsProcessed: resp.GetEventsProcessed(),
		Details:         resp.GetDetails(),
	}, nil
}

// ---- Catalogue (gateway-wide; store binding ignored) ---------------

// CatalogueReloadResult is the diff returned by ReloadCatalogue.
// When Error is non-empty, the live catalogue was NOT mutated.
type CatalogueReloadResult struct {
	Added     []string // cluster_ids newly connected since previous state
	Removed   []string // cluster_ids retired (removed from clusters.eterm)
	Restarted []string // cluster_ids whose connector was restarted
	Error     string   // config-load failure message; empty on success
}

// CatalogueStatus is a snapshot of catalogue state for ops dashboards.
type CatalogueStatus struct {
	CatalogueSize   uint32                 // distinct store_ids across all clusters
	GatewayUptimeMs int64                  // process uptime in milliseconds
	Clusters        []CatalogueClusterInfo // per-connector status
}

// CatalogueClusterInfo describes one cluster connector's state.
type CatalogueClusterInfo struct {
	ClusterID   string    // operator-assigned label from clusters.eterm
	Members     []string  // currently-connected Erlang dist members
	StoreCount  uint32    // distinct store_ids visible from this cluster
	Status      string    // "up" | "degraded" | "unreachable" | "not_yet_connected"
	LastRefresh time.Time // zero when the connector has never reached the cluster
	LastError   string    // empty when Status = "up"; NEVER carries the cookie
}

// ReloadCatalogue triggers the gateway to re-read clusters.eterm and
// reconcile its connectors. Gateway-wide RPC; the Client's store
// binding is ignored.
func (c *Client) ReloadCatalogue(ctx context.Context, opts ...grpc.CallOption) (CatalogueReloadResult, error) {
	resp, err := c.raw.ReloadCatalogue(ctx, &pb.ReloadCatalogueRequest{}, opts...)
	if err != nil {
		return CatalogueReloadResult{}, err
	}
	return CatalogueReloadResult{
		Added:     resp.GetAdded(),
		Removed:   resp.GetRemoved(),
		Restarted: resp.GetRestarted(),
		Error:     resp.GetError(),
	}, nil
}

// GetCatalogueStatus returns a snapshot of the catalogue's current
// state. Gateway-wide RPC; the Client's store binding is ignored.
func (c *Client) GetCatalogueStatus(ctx context.Context, opts ...grpc.CallOption) (CatalogueStatus, error) {
	resp, err := c.raw.GetCatalogueStatus(ctx, &pb.GetCatalogueStatusRequest{}, opts...)
	if err != nil {
		return CatalogueStatus{}, err
	}
	clusters := make([]CatalogueClusterInfo, len(resp.GetClusters()))
	for i, cc := range resp.GetClusters() {
		clusters[i] = catalogueClusterInfoFromProto(cc)
	}
	return CatalogueStatus{
		CatalogueSize:   uint32(resp.GetCatalogueSize()),
		GatewayUptimeMs: resp.GetGatewayUptimeMs(),
		Clusters:        clusters,
	}, nil
}

func catalogueClusterInfoFromProto(p *pb.CatalogueClusterStatus) CatalogueClusterInfo {
	var lastRefresh time.Time
	if s := p.GetLastRefresh(); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			lastRefresh = t
		}
	}
	return CatalogueClusterInfo{
		ClusterID:   p.GetClusterId(),
		Members:     p.GetMembers(),
		StoreCount:  uint32(p.GetStoreCount()),
		Status:      p.GetStatus(),
		LastRefresh: lastRefresh,
		LastError:   p.GetLastError(),
	}
}
