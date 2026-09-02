// Tests for the catalogue-mode admin wrappers. Uses an in-process
// gRPC server over bufconn so we exercise the real wire encode +
// decode + wrapper translation without a live gateway.
package admin

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	pb "github.com/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// --- fake server ----------------------------------------------------

type fakeAdminServer struct {
	pb.UnimplementedAdminServiceServer

	reloadResp *pb.ReloadCatalogueResponse
	reloadErr  error

	statusResp *pb.GetCatalogueStatusResponse
	statusErr  error
}

func (s *fakeAdminServer) ReloadCatalogue(_ context.Context, _ *pb.ReloadCatalogueRequest) (*pb.ReloadCatalogueResponse, error) {
	if s.reloadErr != nil {
		return nil, s.reloadErr
	}
	return s.reloadResp, nil
}

func (s *fakeAdminServer) GetCatalogueStatus(_ context.Context, _ *pb.GetCatalogueStatusRequest) (*pb.GetCatalogueStatusResponse, error) {
	if s.statusErr != nil {
		return nil, s.statusErr
	}
	return s.statusResp, nil
}

// --- test harness ---------------------------------------------------

func newTestClient(t *testing.T, srv *fakeAdminServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterAdminServiceServer(s, srv)
	go func() {
		if err := s.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Logf("server.Serve: %v", err)
		}
	}()
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	return New(conn, "test_store"), func() {
		_ = conn.Close()
		s.Stop()
	}
}

// --- ReloadCatalogue ------------------------------------------------

func TestReloadCatalogue(t *testing.T) {
	tests := []struct {
		name     string
		response *pb.ReloadCatalogueResponse
		want     CatalogueReloadResult
	}{
		{
			name: "full diff",
			response: &pb.ReloadCatalogueResponse{
				Added:     []string{"cluster_a", "cluster_b"},
				Removed:   []string{"cluster_c"},
				Restarted: []string{"cluster_d", "cluster_e"},
				Error:     "",
			},
			want: CatalogueReloadResult{
				Added:     []string{"cluster_a", "cluster_b"},
				Removed:   []string{"cluster_c"},
				Restarted: []string{"cluster_d", "cluster_e"},
				Error:     "",
			},
		},
		{
			name:     "no changes",
			response: &pb.ReloadCatalogueResponse{},
			want:     CatalogueReloadResult{},
		},
		{
			name: "config load failure (no mutation)",
			response: &pb.ReloadCatalogueResponse{
				Error: "duplicate cluster_id: parksim",
			},
			want: CatalogueReloadResult{
				Error: "duplicate cluster_id: parksim",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, cleanup := newTestClient(t, &fakeAdminServer{reloadResp: tc.response})
			defer cleanup()

			got, err := c.ReloadCatalogue(context.Background())
			if err != nil {
				t.Fatalf("ReloadCatalogue: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("result mismatch\n  got:  %+v\n  want: %+v", got, tc.want)
			}
		})
	}
}

func TestReloadCatalogue_RPCError(t *testing.T) {
	srv := &fakeAdminServer{
		reloadErr: status.Error(codes.Internal, "boom"),
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	got, err := c.ReloadCatalogue(context.Background())
	if err == nil {
		t.Fatalf("ReloadCatalogue: want error, got nil (result=%+v)", got)
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Internal {
		t.Errorf("error code: got %v, want Internal", err)
	}
	if !reflect.DeepEqual(got, CatalogueReloadResult{}) {
		t.Errorf("on error, result should be zero value; got %+v", got)
	}
}

// --- GetCatalogueStatus ---------------------------------------------

func TestGetCatalogueStatus(t *testing.T) {
	refresh1 := "2026-05-20T01:23:45Z"
	tests := []struct {
		name     string
		response *pb.GetCatalogueStatusResponse
		want     CatalogueStatus
	}{
		{
			name:     "empty catalogue (no clusters configured)",
			response: &pb.GetCatalogueStatusResponse{},
			want: CatalogueStatus{
				Clusters: []CatalogueClusterInfo{},
			},
		},
		{
			name: "single up cluster with timestamp",
			response: &pb.GetCatalogueStatusResponse{
				CatalogueSize:   3,
				GatewayUptimeMs: 123_456,
				Clusters: []*pb.CatalogueClusterStatus{
					{
						ClusterId:   "parksim",
						Members:     []string{"parksim_entry2exit@192.168.1.10", "parksim_lot@192.168.1.11"},
						StoreCount:  3,
						Status:      "up",
						LastRefresh: refresh1,
					},
				},
			},
			want: CatalogueStatus{
				CatalogueSize:   3,
				GatewayUptimeMs: 123_456,
				Clusters: []CatalogueClusterInfo{
					{
						ClusterID:   "parksim",
						Members:     []string{"parksim_entry2exit@192.168.1.10", "parksim_lot@192.168.1.11"},
						StoreCount:  3,
						Status:      "up",
						LastRefresh: time.Date(2026, 5, 20, 1, 23, 45, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "multiple clusters mixed status",
			response: &pb.GetCatalogueStatusResponse{
				CatalogueSize: 5,
				Clusters: []*pb.CatalogueClusterStatus{
					{
						ClusterId:  "alpha",
						Status:     "up",
						StoreCount: 2,
					},
					{
						ClusterId:  "beta",
						Status:     "degraded",
						StoreCount: 3,
						LastError:  "1 of 3 members unreachable",
					},
					{
						ClusterId: "gamma",
						Status:    "not_yet_connected",
					},
				},
			},
			want: CatalogueStatus{
				CatalogueSize: 5,
				Clusters: []CatalogueClusterInfo{
					{ClusterID: "alpha", Status: "up", StoreCount: 2},
					{
						ClusterID:  "beta",
						Status:     "degraded",
						StoreCount: 3,
						LastError:  "1 of 3 members unreachable",
					},
					{ClusterID: "gamma", Status: "not_yet_connected"},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, cleanup := newTestClient(t, &fakeAdminServer{statusResp: tc.response})
			defer cleanup()

			got, err := c.GetCatalogueStatus(context.Background())
			if err != nil {
				t.Fatalf("GetCatalogueStatus: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("result mismatch\n  got:  %+v\n  want: %+v", got, tc.want)
			}
		})
	}
}

func TestGetCatalogueStatus_RPCError(t *testing.T) {
	srv := &fakeAdminServer{
		statusErr: status.Error(codes.Unavailable, "catalogue not ready"),
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	got, err := c.GetCatalogueStatus(context.Background())
	if err == nil {
		t.Fatalf("GetCatalogueStatus: want error, got nil (result=%+v)", got)
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.Unavailable {
		t.Errorf("error code: got %v, want Unavailable", err)
	}
	if !reflect.DeepEqual(got, CatalogueStatus{}) {
		t.Errorf("on error, result should be zero value; got %+v", got)
	}
}

// --- catalogueClusterInfoFromProto ---------------------------------
//
// Direct unit tests for the proto-to-typed helper. Most cases are
// exercised by the round-trip tests above; this block covers
// timestamp parsing edge cases that would otherwise be silent.

func TestCatalogueClusterInfoFromProto_TimestampParsing(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		wantRefresh time.Time
	}{
		{"valid Z UTC", "2026-05-20T01:23:45Z",
			time.Date(2026, 5, 20, 1, 23, 45, 0, time.UTC)},
		{"valid offset", "2026-05-20T03:23:45+02:00",
			time.Date(2026, 5, 20, 3, 23, 45, 0, time.FixedZone("", 2*3600))},
		{"empty -> zero time", "", time.Time{}},
		{"malformed -> zero time", "yesterday", time.Time{}},
		{"truncated -> zero time", "2026-05-20", time.Time{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := catalogueClusterInfoFromProto(&pb.CatalogueClusterStatus{
				ClusterId:   "x",
				LastRefresh: tc.raw,
			})
			if !got.LastRefresh.Equal(tc.wantRefresh) {
				t.Errorf("LastRefresh: got %v, want %v", got.LastRefresh, tc.wantRefresh)
			}
		})
	}
}

func TestCatalogueClusterInfoFromProto_NilSafe(t *testing.T) {
	// Proto-generated getters handle nil receivers; the helper must
	// not panic on a nil pointer (could happen if the server returns
	// nil entries in the clusters slice).
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked on nil proto: %v", r)
		}
	}()
	got := catalogueClusterInfoFromProto(nil)
	if !reflect.DeepEqual(got, CatalogueClusterInfo{}) {
		t.Errorf("nil proto should map to zero value; got %+v", got)
	}
}
