package temporal

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeTemporalServer struct {
	pb.UnimplementedTemporalServiceServer

	gotUntilReq *pb.ReadUntilRequest
	untilResp   *pb.ReadUntilResponse
	untilErr    error

	gotRangeReq *pb.ReadRangeRequest
	rangeResp   *pb.ReadRangeResponse
	rangeErr    error

	gotVersionAtReq *pb.VersionAtRequest
	versionAtResp   *pb.VersionAtResponse
	versionAtErr    error
}

func (s *fakeTemporalServer) ReadUntil(_ context.Context, req *pb.ReadUntilRequest) (*pb.ReadUntilResponse, error) {
	s.gotUntilReq = req
	if s.untilErr != nil {
		return nil, s.untilErr
	}
	return s.untilResp, nil
}

func (s *fakeTemporalServer) ReadRange(_ context.Context, req *pb.ReadRangeRequest) (*pb.ReadRangeResponse, error) {
	s.gotRangeReq = req
	if s.rangeErr != nil {
		return nil, s.rangeErr
	}
	return s.rangeResp, nil
}

func (s *fakeTemporalServer) VersionAt(_ context.Context, req *pb.VersionAtRequest) (*pb.VersionAtResponse, error) {
	s.gotVersionAtReq = req
	if s.versionAtErr != nil {
		return nil, s.versionAtErr
	}
	return s.versionAtResp, nil
}

func newTestClient(t *testing.T, srv *fakeTemporalServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterTemporalServiceServer(s, srv)
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

func TestUntil(t *testing.T) {
	srv := &fakeTemporalServer{
		untilResp: &pb.ReadUntilResponse{
			Events: []*pb.RecordedEvent{
				{EventId: "e1", EventType: "ping_v1", Version: 0, Timestamp: 1000},
				{EventId: "e2", EventType: "ping_v1", Version: 1, Timestamp: 2000},
			},
		},
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	cutoff := time.UnixMilli(3000)
	got, err := c.Until(context.Background(), "s1", cutoff, 100)
	if err != nil {
		t.Fatalf("Until: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
	if got[0].EventID != "e1" || got[1].EventID != "e2" {
		t.Errorf("events: %+v", got)
	}
	if srv.gotUntilReq.GetStoreId() != "test_store" || srv.gotUntilReq.GetStreamId() != "s1" {
		t.Errorf("request: %+v", srv.gotUntilReq)
	}
	if srv.gotUntilReq.GetTimestamp() != 3000 {
		t.Errorf("cutoff ms: got %d want 3000", srv.gotUntilReq.GetTimestamp())
	}
}

func TestUntil_RPCError(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeTemporalServer{untilErr: status.Error(codes.Internal, "x")})
	defer cleanup()
	if _, err := c.Until(context.Background(), "s1", time.Now(), 10); err == nil {
		t.Fatalf("want error")
	}
}

func TestRange(t *testing.T) {
	srv := &fakeTemporalServer{rangeResp: &pb.ReadRangeResponse{
		Events: []*pb.RecordedEvent{{EventId: "e1"}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	from := time.UnixMilli(1000)
	to := time.UnixMilli(5000)
	got, err := c.Range(context.Background(), "s1", from, to, 50)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	if srv.gotRangeReq.GetFromTimestamp() != 1000 || srv.gotRangeReq.GetToTimestamp() != 5000 {
		t.Errorf("range request bounds: %+v", srv.gotRangeReq)
	}
}

func TestVersionAt(t *testing.T) {
	srv := &fakeTemporalServer{versionAtResp: &pb.VersionAtResponse{Version: 42}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.VersionAt(context.Background(), "s1", time.UnixMilli(1234))
	if err != nil {
		t.Fatalf("VersionAt: %v", err)
	}
	if got != 42 {
		t.Errorf("got %d want 42", got)
	}
	if srv.gotVersionAtReq.GetTimestamp() != 1234 {
		t.Errorf("ts: %d", srv.gotVersionAtReq.GetTimestamp())
	}
}

func TestVersionAt_RPCError(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeTemporalServer{versionAtErr: status.Error(codes.NotFound, "x")})
	defer cleanup()
	if _, err := c.VersionAt(context.Background(), "s1", time.Now()); err == nil {
		t.Fatalf("want error")
	}
}

func TestEventTimestampMapping(t *testing.T) {
	srv := &fakeTemporalServer{untilResp: &pb.ReadUntilResponse{
		Events: []*pb.RecordedEvent{{Timestamp: 1716200000000, EpochUs: 1716200000000000}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Until(context.Background(), "s1", time.Now(), 10)
	if err != nil {
		t.Fatalf("Until: %v", err)
	}
	if !got[0].Timestamp.Equal(time.UnixMilli(1716200000000)) {
		t.Errorf("Timestamp: got %v", got[0].Timestamp)
	}
	if !got[0].Epoch.Equal(time.UnixMicro(1716200000000000)) {
		t.Errorf("Epoch: got %v", got[0].Epoch)
	}
}
