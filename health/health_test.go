package health

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeHealthServer struct {
	pb.UnimplementedHealthServiceServer

	checkResp *pb.HealthCheckResponse
	checkErr  error

	healthResp *pb.HealthResponse
	healthErr  error

	clusterResp *pb.ClusterCheckResponse
	clusterErr  error

	memLevelResp *pb.MemoryLevelResponse
	memLevelErr  error

	memStatsResp *pb.MemoryStatsResponse
	memStatsErr  error

	serverInfoResp *pb.ServerInfoResponse
	serverInfoErr  error
}

func (s *fakeHealthServer) Check(_ context.Context, _ *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	if s.checkErr != nil {
		return nil, s.checkErr
	}
	return s.checkResp, nil
}
func (s *fakeHealthServer) Health(_ context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	if s.healthErr != nil {
		return nil, s.healthErr
	}
	return s.healthResp, nil
}
func (s *fakeHealthServer) VerifyClusterConsistency(_ context.Context, _ *pb.ClusterCheckRequest) (*pb.ClusterCheckResponse, error) {
	if s.clusterErr != nil {
		return nil, s.clusterErr
	}
	return s.clusterResp, nil
}
func (s *fakeHealthServer) VerifyMembershipConsensus(_ context.Context, _ *pb.ClusterCheckRequest) (*pb.ClusterCheckResponse, error) {
	if s.clusterErr != nil {
		return nil, s.clusterErr
	}
	return s.clusterResp, nil
}
func (s *fakeHealthServer) CheckRaftLogConsistency(_ context.Context, _ *pb.ClusterCheckRequest) (*pb.ClusterCheckResponse, error) {
	if s.clusterErr != nil {
		return nil, s.clusterErr
	}
	return s.clusterResp, nil
}
func (s *fakeHealthServer) GetMemoryLevel(_ context.Context, _ *pb.MemoryLevelRequest) (*pb.MemoryLevelResponse, error) {
	if s.memLevelErr != nil {
		return nil, s.memLevelErr
	}
	return s.memLevelResp, nil
}
func (s *fakeHealthServer) GetMemoryStats(_ context.Context, _ *pb.MemoryStatsRequest) (*pb.MemoryStatsResponse, error) {
	if s.memStatsErr != nil {
		return nil, s.memStatsErr
	}
	return s.memStatsResp, nil
}
func (s *fakeHealthServer) GetServerInfo(_ context.Context, _ *pb.GetServerInfoRequest) (*pb.ServerInfoResponse, error) {
	if s.serverInfoErr != nil {
		return nil, s.serverInfoErr
	}
	return s.serverInfoResp, nil
}

func newTestClient(t *testing.T, srv *fakeHealthServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterHealthServiceServer(s, srv)
	go func() {
		if err := s.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Logf("server.Serve: %v", err)
		}
	}()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	return New(conn, "test_store"), func() { _ = conn.Close(); s.Stop() }
}

func TestCheck(t *testing.T) {
	cases := map[pb.HealthStatus]Status{
		pb.HealthStatus_HEALTH_STATUS_HEALTHY:   StatusHealthy,
		pb.HealthStatus_HEALTH_STATUS_DEGRADED:  StatusDegraded,
		pb.HealthStatus_HEALTH_STATUS_UNHEALTHY: StatusUnhealthy,
		pb.HealthStatus(99):                     StatusUnknown,
	}
	for in, want := range cases {
		t.Run(want.string(), func(t *testing.T) {
			srv := &fakeHealthServer{checkResp: &pb.HealthCheckResponse{
				Status:  in,
				Details: map[string]string{"k": "v"},
			}}
			c, cleanup := newTestClient(t, srv)
			defer cleanup()
			got, err := c.Check(context.Background())
			if err != nil {
				t.Fatalf("Check: %v", err)
			}
			if got.Status != want {
				t.Errorf("status: got %v want %v", got.Status, want)
			}
			if got.Details["k"] != "v" {
				t.Errorf("details lost: %+v", got.Details)
			}
		})
	}
}

func (s Status) string() string { return string(s) }

func TestHealth_GatewayWide(t *testing.T) {
	srv := &fakeHealthServer{healthResp: &pb.HealthResponse{
		Status:       pb.HealthStatus_HEALTH_STATUS_HEALTHY,
		Stores:       map[string]uint32{"a": 3, "b": 5},
		TotalWorkers: 8,
		Node:         "reckon_gateway@192.168.1.11",
		Timestamp:    1716200000000,
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	want := HealthResult{
		Status:       StatusHealthy,
		Stores:       map[string]uint32{"a": 3, "b": 5},
		TotalWorkers: 8,
		Node:         "reckon_gateway@192.168.1.11",
		Timestamp:    time.UnixMilli(1716200000000),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
}

func TestClusterChecks(t *testing.T) {
	srv := &fakeHealthServer{clusterResp: &pb.ClusterCheckResponse{
		Status:  pb.ClusterStatus_CLUSTER_STATUS_DEGRADED,
		Details: map[string]string{"reason": "1 member down"},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	for name, fn := range map[string]func(context.Context, ...grpc.CallOption) (ClusterResult, error){
		"ClusterConsistency":  c.ClusterConsistency,
		"MembershipConsensus": c.MembershipConsensus,
		"RaftLogConsistency":  c.RaftLogConsistency,
	} {
		t.Run(name, func(t *testing.T) {
			got, err := fn(context.Background())
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if got.Status != ClusterDegraded {
				t.Errorf("status: got %v want degraded", got.Status)
			}
			if got.Details["reason"] != "1 member down" {
				t.Errorf("details: %+v", got.Details)
			}
		})
	}
}

func TestMemoryLevel(t *testing.T) {
	cases := map[pb.MemoryLevel]MemoryLevel{
		pb.MemoryLevel_MEMORY_LEVEL_NORMAL:   MemoryNormal,
		pb.MemoryLevel_MEMORY_LEVEL_HIGH:     MemoryHigh,
		pb.MemoryLevel_MEMORY_LEVEL_CRITICAL: MemoryCritical,
	}
	for in, want := range cases {
		srv := &fakeHealthServer{memLevelResp: &pb.MemoryLevelResponse{Level: in}}
		c, cleanup := newTestClient(t, srv)
		got, err := c.MemoryLevel(context.Background())
		cleanup()
		if err != nil {
			t.Fatalf("MemoryLevel: %v", err)
		}
		if got != want {
			t.Errorf("%v: got %v want %v", in, got, want)
		}
	}
}

func TestMemoryStats(t *testing.T) {
	srv := &fakeHealthServer{memStatsResp: &pb.MemoryStatsResponse{
		UsedBytes:    1024,
		TotalBytes:   4096,
		UsagePercent: 25.0,
		Breakdown:    map[string]uint64{"ets": 512, "heap": 512},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.MemoryStats(context.Background())
	if err != nil {
		t.Fatalf("MemoryStats: %v", err)
	}
	if got.UsedBytes != 1024 || got.UsagePercent != 25.0 || got.Breakdown["heap"] != 512 {
		t.Errorf("got %+v", got)
	}
}

func TestServerInfo(t *testing.T) {
	srv := &fakeHealthServer{serverInfoResp: &pb.ServerInfoResponse{
		ReckonDbVersion:         "2.3.7",
		ReckonGatewayVersion:    "0.5.4",
		ApiCompatibilityVersion: "reckon.gateway.v1",
		IntegrityEnabled:        true,
		IntegrityAlgo:           "sha256-deterministic-etf-v1",
		HmacKeyId:               1,
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.ServerInfo(context.Background())
	if err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	if got.ReckonDbVersion != "2.3.7" || !got.IntegrityEnabled || got.HmacKeyId != 1 {
		t.Errorf("got %+v", got)
	}
}

func TestAllRPCErrors(t *testing.T) {
	srv := &fakeHealthServer{
		checkErr:       status.Error(codes.Internal, "x"),
		healthErr:      status.Error(codes.Internal, "x"),
		clusterErr:     status.Error(codes.Internal, "x"),
		memLevelErr:    status.Error(codes.Internal, "x"),
		memStatsErr:    status.Error(codes.Internal, "x"),
		serverInfoErr:  status.Error(codes.Internal, "x"),
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	ctx := context.Background()
	if _, err := c.Check(ctx); err == nil {
		t.Error("Check: want error")
	}
	if _, err := c.Health(ctx); err == nil {
		t.Error("Health: want error")
	}
	if _, err := c.ClusterConsistency(ctx); err == nil {
		t.Error("ClusterConsistency: want error")
	}
	if _, err := c.MembershipConsensus(ctx); err == nil {
		t.Error("MembershipConsensus: want error")
	}
	if _, err := c.RaftLogConsistency(ctx); err == nil {
		t.Error("RaftLogConsistency: want error")
	}
	if _, err := c.MemoryLevel(ctx); err == nil {
		t.Error("MemoryLevel: want error")
	}
	if _, err := c.MemoryStats(ctx); err == nil {
		t.Error("MemoryStats: want error")
	}
	if _, err := c.ServerInfo(ctx); err == nil {
		t.Error("ServerInfo: want error")
	}
}
