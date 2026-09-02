package snapshots

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

type fakeSnapshotServer struct {
	pb.UnimplementedSnapshotServiceServer

	gotSaveReq *pb.RecordSnapshotRequest
	saveErr    error

	readResp *pb.SnapshotRecord
	readErr  error

	deleteErr error

	listResp *pb.ListSnapshotsResponse
	listErr  error

	listAllResp *pb.ListSnapshotsResponse
	listAllErr  error
}

func (s *fakeSnapshotServer) RecordSnapshot(_ context.Context, req *pb.RecordSnapshotRequest) (*pb.RecordSnapshotResponse, error) {
	s.gotSaveReq = req
	if s.saveErr != nil {
		return nil, s.saveErr
	}
	return &pb.RecordSnapshotResponse{}, nil
}
func (s *fakeSnapshotServer) ReadSnapshot(_ context.Context, _ *pb.ReadSnapshotRequest) (*pb.SnapshotRecord, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	return s.readResp, nil
}
func (s *fakeSnapshotServer) DeleteSnapshot(_ context.Context, _ *pb.DeleteSnapshotRequest) (*pb.DeleteSnapshotResponse, error) {
	if s.deleteErr != nil {
		return nil, s.deleteErr
	}
	return &pb.DeleteSnapshotResponse{}, nil
}
func (s *fakeSnapshotServer) ListSnapshots(_ context.Context, _ *pb.ListSnapshotsRequest) (*pb.ListSnapshotsResponse, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.listResp, nil
}
func (s *fakeSnapshotServer) ListAllSnapshots(_ context.Context, _ *pb.ListAllSnapshotsRequest) (*pb.ListSnapshotsResponse, error) {
	if s.listAllErr != nil {
		return nil, s.listAllErr
	}
	return s.listAllResp, nil
}

func newTestClient(t *testing.T, srv *fakeSnapshotServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterSnapshotServiceServer(s, srv)
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

func TestSave_PropagatesAllFields(t *testing.T) {
	srv := &fakeSnapshotServer{}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	if err := c.Save(context.Background(), Spec{
		SourceUUID: "src", StreamUUID: "stream-1", Version: 42,
		Data: []byte("state"), Metadata: []byte("meta"),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if srv.gotSaveReq.GetVersion() != 42 ||
		srv.gotSaveReq.GetSourceUuid() != "src" ||
		srv.gotSaveReq.GetStreamUuid() != "stream-1" ||
		string(srv.gotSaveReq.GetData()) != "state" ||
		string(srv.gotSaveReq.GetMetadata()) != "meta" {
		t.Errorf("request fields not propagated: %+v", srv.gotSaveReq)
	}
}

func TestSave_Error(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeSnapshotServer{saveErr: status.Error(codes.Internal, "x")})
	defer cleanup()
	if err := c.Save(context.Background(), Spec{}); err == nil {
		t.Fatalf("want error")
	}
}

func TestAt(t *testing.T) {
	srv := &fakeSnapshotServer{readResp: &pb.SnapshotRecord{
		StreamId: "users-42",
		Version:  100,
		Data:     []byte("payload"),
		Metadata: []byte("meta"),
		Timestamp: 1716200000000,
		AnchorHash: []byte{0xde, 0xad},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.At(context.Background(), "users", "users-42", 100)
	if err != nil {
		t.Fatalf("At: %v", err)
	}
	want := Record{
		StreamID: "users-42", Version: 100,
		Data: []byte("payload"), Metadata: []byte("meta"),
		Timestamp:  time.UnixMilli(1716200000000),
		AnchorHash: []byte{0xde, 0xad},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestLatest_PicksHighestVersion(t *testing.T) {
	// Latest is client-side List+max so the test stresses that logic
	srv := &fakeSnapshotServer{listResp: &pb.ListSnapshotsResponse{
		Snapshots: []*pb.SnapshotRecord{
			{StreamId: "s", Version: 5},
			{StreamId: "s", Version: 12},
			{StreamId: "s", Version: 3},
		},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Latest(context.Background(), "src", "s")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got.Version != 12 {
		t.Errorf("got version %d want 12", got.Version)
	}
}

func TestLatest_EmptyListReturnsZero(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeSnapshotServer{listResp: &pb.ListSnapshotsResponse{}})
	defer cleanup()
	got, err := c.Latest(context.Background(), "src", "s")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got.StreamID != "" || got.Version != 0 {
		t.Errorf("expected zero Record, got %+v", got)
	}
}

func TestDelete(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeSnapshotServer{})
	defer cleanup()
	if err := c.Delete(context.Background(), "src", "s", 1); err != nil {
		t.Errorf("Delete: %v", err)
	}
}

func TestList(t *testing.T) {
	srv := &fakeSnapshotServer{listResp: &pb.ListSnapshotsResponse{
		Snapshots: []*pb.SnapshotRecord{{StreamId: "s", Version: 1}, {StreamId: "s", Version: 2}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.List(context.Background(), "src", "s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d want 2", len(got))
	}
}

func TestListAll(t *testing.T) {
	srv := &fakeSnapshotServer{listAllResp: &pb.ListSnapshotsResponse{
		Snapshots: []*pb.SnapshotRecord{{StreamId: "a"}, {StreamId: "b"}, {StreamId: "c"}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.ListAll(context.Background())
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("got %d want 3", len(got))
	}
}
