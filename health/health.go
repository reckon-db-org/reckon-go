// Package health wraps the gRPC HealthService with idiomatic Go types.
//
// Two flavours of methods:
//
//   - Per-store checks (Check, Cluster*, Memory*, ServerInfo) take a
//     store id. Construct a Client with [New] bound to that store.
//   - Gateway-wide check (Health) hits every store the gateway knows
//     about — it ignores the bound store id but uses the same Client
//     for convenience.
//
// Usage:
//
//	c, _ := reckon.Connect(ctx, "beam01.lab:50051")
//	defer c.Close()
//	h := c.Health("default_store")
//
//	st, _ := h.Check(ctx)
//	info, _ := h.ServerInfo(ctx)
//	mem, _ := h.MemoryStats(ctx)
package health

import (
	"context"
	"time"

	pb "github.com/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
)

// Status is the high-level liveness/health verdict for a store or for
// the gateway. Map to the strings used in gRPC `details` for display.
type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusDegraded  Status = "degraded"
	StatusUnhealthy Status = "unhealthy"
	StatusUnknown   Status = "unknown"
)

func statusFromProto(s pb.HealthStatus) Status {
	switch s {
	case pb.HealthStatus_HEALTH_STATUS_HEALTHY:
		return StatusHealthy
	case pb.HealthStatus_HEALTH_STATUS_DEGRADED:
		return StatusDegraded
	case pb.HealthStatus_HEALTH_STATUS_UNHEALTHY:
		return StatusUnhealthy
	default:
		return StatusUnknown
	}
}

// ClusterStatus is the verdict of a cluster-level check.
type ClusterStatus string

const (
	ClusterHealthy   ClusterStatus = "healthy"
	ClusterDegraded  ClusterStatus = "degraded"
	ClusterUnhealthy ClusterStatus = "unhealthy"
)

func clusterStatusFromProto(s pb.ClusterStatus) ClusterStatus {
	switch s {
	case pb.ClusterStatus_CLUSTER_STATUS_HEALTHY:
		return ClusterHealthy
	case pb.ClusterStatus_CLUSTER_STATUS_DEGRADED:
		return ClusterDegraded
	default:
		return ClusterUnhealthy
	}
}

// MemoryLevel is the coarse memory-pressure bucket for a store.
type MemoryLevel string

const (
	MemoryNormal   MemoryLevel = "normal"
	MemoryHigh     MemoryLevel = "high"
	MemoryCritical MemoryLevel = "critical"
)

func memoryLevelFromProto(l pb.MemoryLevel) MemoryLevel {
	switch l {
	case pb.MemoryLevel_MEMORY_LEVEL_HIGH:
		return MemoryHigh
	case pb.MemoryLevel_MEMORY_LEVEL_CRITICAL:
		return MemoryCritical
	default:
		return MemoryNormal
	}
}

// CheckResult is the per-store result of [Client.Check].
type CheckResult struct {
	Status  Status
	Details map[string]string
}

// HealthResult is the gateway-wide result of [Client.Health].
type HealthResult struct {
	Status       Status
	Stores       map[string]uint32 // store id → worker count
	TotalWorkers uint32
	Node         string
	Timestamp    time.Time
}

// ClusterResult is the result of any cluster-level diagnostic
// (consistency / membership / raft log).
type ClusterResult struct {
	Status  ClusterStatus
	Details map[string]string
}

// MemoryStats is the result of [Client.MemoryStats].
type MemoryStats struct {
	UsedBytes    uint64
	TotalBytes   uint64
	UsagePercent float64
	Breakdown    map[string]uint64
}

// ServerInfo is the result of [Client.ServerInfo].
type ServerInfo struct {
	ReckonDbVersion        string
	ReckonGatewayVersion   string
	APICompatibilityVersion string
	IntegrityEnabled       bool
	IntegrityAlgo          string
	HmacKeyId              uint32
}

// Client is the typed wrapper for one store. Construct via [New].
type Client struct {
	raw   pb.HealthServiceClient
	store string
}

// New returns a Client that issues RPCs over conn, scoped to storeID.
// Gateway-wide methods (e.g. [Client.Health]) ignore the binding.
func New(conn grpc.ClientConnInterface, storeID string) *Client {
	return &Client{raw: pb.NewHealthServiceClient(conn), store: storeID}
}

// StoreID returns the store this Client is bound to.
func (c *Client) StoreID() string { return c.store }

// Check returns a quick liveness probe for the bound store.
func (c *Client) Check(ctx context.Context, opts ...grpc.CallOption) (CheckResult, error) {
	resp, err := c.raw.Check(ctx, &pb.HealthCheckRequest{StoreId: c.store}, opts...)
	if err != nil {
		return CheckResult{}, err
	}
	return CheckResult{
		Status:  statusFromProto(resp.GetStatus()),
		Details: resp.GetDetails(),
	}, nil
}

// Health returns the gateway-wide health snapshot, listing every
// store the gateway is hosting and the total worker count.
func (c *Client) Health(ctx context.Context, opts ...grpc.CallOption) (HealthResult, error) {
	resp, err := c.raw.Health(ctx, &pb.HealthRequest{}, opts...)
	if err != nil {
		return HealthResult{}, err
	}
	return HealthResult{
		Status:       statusFromProto(resp.GetStatus()),
		Stores:       resp.GetStores(),
		TotalWorkers: resp.GetTotalWorkers(),
		Node:         resp.GetNode(),
		Timestamp:    time.UnixMilli(resp.GetTimestamp()),
	}, nil
}

// ClusterConsistency verifies the cluster-level view of the bound
// store. Combines leader, membership, and quorum checks.
func (c *Client) ClusterConsistency(ctx context.Context, opts ...grpc.CallOption) (ClusterResult, error) {
	return c.clusterCheck(ctx, c.raw.VerifyClusterConsistency, opts)
}

// MembershipConsensus verifies that all reachable cluster nodes agree
// on who owns the bound store.
func (c *Client) MembershipConsensus(ctx context.Context, opts ...grpc.CallOption) (ClusterResult, error) {
	return c.clusterCheck(ctx, c.raw.VerifyMembershipConsensus, opts)
}

// RaftLogConsistency verifies that the Raft log is internally
// consistent across cluster nodes for the bound store.
func (c *Client) RaftLogConsistency(ctx context.Context, opts ...grpc.CallOption) (ClusterResult, error) {
	return c.clusterCheck(ctx, c.raw.CheckRaftLogConsistency, opts)
}

type clusterFn func(context.Context, *pb.ClusterCheckRequest, ...grpc.CallOption) (*pb.ClusterCheckResponse, error)

func (c *Client) clusterCheck(ctx context.Context, fn clusterFn, opts []grpc.CallOption) (ClusterResult, error) {
	resp, err := fn(ctx, &pb.ClusterCheckRequest{StoreId: c.store}, opts...)
	if err != nil {
		return ClusterResult{}, err
	}
	return ClusterResult{
		Status:  clusterStatusFromProto(resp.GetStatus()),
		Details: resp.GetDetails(),
	}, nil
}

// MemoryLevel returns the coarse memory-pressure bucket for the
// bound store.
func (c *Client) MemoryLevel(ctx context.Context, opts ...grpc.CallOption) (MemoryLevel, error) {
	resp, err := c.raw.GetMemoryLevel(ctx, &pb.MemoryLevelRequest{StoreId: c.store}, opts...)
	if err != nil {
		return MemoryNormal, err
	}
	return memoryLevelFromProto(resp.GetLevel()), nil
}

// MemoryStats returns detailed memory accounting for the bound store.
func (c *Client) MemoryStats(ctx context.Context, opts ...grpc.CallOption) (MemoryStats, error) {
	resp, err := c.raw.GetMemoryStats(ctx, &pb.MemoryStatsRequest{StoreId: c.store}, opts...)
	if err != nil {
		return MemoryStats{}, err
	}
	return MemoryStats{
		UsedBytes:    resp.GetUsedBytes(),
		TotalBytes:   resp.GetTotalBytes(),
		UsagePercent: resp.GetUsagePercent(),
		Breakdown:    resp.GetBreakdown(),
	}, nil
}

// ServerInfo returns gateway + db versions and integrity config.
func (c *Client) ServerInfo(ctx context.Context, opts ...grpc.CallOption) (ServerInfo, error) {
	resp, err := c.raw.GetServerInfo(ctx, &pb.GetServerInfoRequest{StoreId: c.store}, opts...)
	if err != nil {
		return ServerInfo{}, err
	}
	return ServerInfo{
		ReckonDbVersion:         resp.GetReckonDbVersion(),
		ReckonGatewayVersion:    resp.GetReckonGatewayVersion(),
		APICompatibilityVersion: resp.GetApiCompatibilityVersion(),
		IntegrityEnabled:        resp.GetIntegrityEnabled(),
		IntegrityAlgo:           resp.GetIntegrityAlgo(),
		HmacKeyId:               resp.GetHmacKeyId(),
	}, nil
}
