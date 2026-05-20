package streams

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

type fakeStreamServer struct {
	pb.UnimplementedStreamServiceServer

	gotAppendReq *pb.AppendEventsRequest
	appendResp   *pb.AppendEventsResponse
	appendErr    error

	gotReadFwdReq *pb.ReadStreamRequest
	readFwdResp   *pb.ReadStreamResponse
	readFwdErr    error

	gotReadBwdReq *pb.ReadStreamRequest
	readBwdResp   *pb.ReadStreamResponse
	readBwdErr    error

	versionResp *pb.GetStreamVersionResponse
	versionErr  error

	listResp *pb.ListStreamsResponse
	listErr  error

	deleteErr error

	gotByTypesReq *pb.ReadByEventTypesRequest
	byTypesResp   *pb.ReadStreamResponse
	byTypesErr    error

	gotByTagsReq *pb.ReadByTagsRequest
	byTagsResp   *pb.ReadStreamResponse
	byTagsErr    error

	globalResp *pb.ReadStreamResponse
	globalErr  error

	watchEvents []*pb.RecordedEvent
	watchErr    error
}

func (s *fakeStreamServer) AppendEvents(_ context.Context, req *pb.AppendEventsRequest) (*pb.AppendEventsResponse, error) {
	s.gotAppendReq = req
	if s.appendErr != nil {
		return nil, s.appendErr
	}
	return s.appendResp, nil
}
func (s *fakeStreamServer) ReadStreamForward(_ context.Context, req *pb.ReadStreamRequest) (*pb.ReadStreamResponse, error) {
	s.gotReadFwdReq = req
	if s.readFwdErr != nil {
		return nil, s.readFwdErr
	}
	return s.readFwdResp, nil
}
func (s *fakeStreamServer) ReadStreamBackward(_ context.Context, req *pb.ReadStreamRequest) (*pb.ReadStreamResponse, error) {
	s.gotReadBwdReq = req
	if s.readBwdErr != nil {
		return nil, s.readBwdErr
	}
	return s.readBwdResp, nil
}
func (s *fakeStreamServer) GetStreamVersion(_ context.Context, _ *pb.GetStreamVersionRequest) (*pb.GetStreamVersionResponse, error) {
	if s.versionErr != nil {
		return nil, s.versionErr
	}
	return s.versionResp, nil
}
func (s *fakeStreamServer) ListStreams(_ context.Context, _ *pb.ListStreamsRequest) (*pb.ListStreamsResponse, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.listResp, nil
}
func (s *fakeStreamServer) DeleteStream(_ context.Context, _ *pb.DeleteStreamRequest) (*pb.DeleteStreamResponse, error) {
	if s.deleteErr != nil {
		return nil, s.deleteErr
	}
	return &pb.DeleteStreamResponse{}, nil
}
func (s *fakeStreamServer) ReadByEventTypes(_ context.Context, req *pb.ReadByEventTypesRequest) (*pb.ReadStreamResponse, error) {
	s.gotByTypesReq = req
	if s.byTypesErr != nil {
		return nil, s.byTypesErr
	}
	return s.byTypesResp, nil
}
func (s *fakeStreamServer) ReadByTags(_ context.Context, req *pb.ReadByTagsRequest) (*pb.ReadStreamResponse, error) {
	s.gotByTagsReq = req
	if s.byTagsErr != nil {
		return nil, s.byTagsErr
	}
	return s.byTagsResp, nil
}
func (s *fakeStreamServer) ReadAllGlobal(_ context.Context, _ *pb.ReadAllGlobalRequest) (*pb.ReadStreamResponse, error) {
	if s.globalErr != nil {
		return nil, s.globalErr
	}
	return s.globalResp, nil
}
func (s *fakeStreamServer) StreamEventsForward(_ *pb.ReadStreamRequest, srv pb.StreamService_StreamEventsForwardServer) error {
	for _, ev := range s.watchEvents {
		if err := srv.Send(ev); err != nil {
			return err
		}
	}
	return s.watchErr
}

func newTestClient(t *testing.T, srv *fakeStreamServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterStreamServiceServer(s, srv)
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

func TestAppend_PropagatesExpectedVersion(t *testing.T) {
	srv := &fakeStreamServer{appendResp: &pb.AppendEventsResponse{Version: 7, Position: 100, Count: 2}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	res, err := c.Append(context.Background(), "users-42", AnyVersion, []ProposedEvent{
		{EventType: "user_registered_v1", Data: []byte("d1"), Tags: []string{"t1"}},
		{EventType: "user_promoted_v1", Data: []byte("d2")},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if res.Version != 7 || res.Position != 100 || res.Count != 2 {
		t.Errorf("result %+v", res)
	}
	if srv.gotAppendReq.GetExpectedVersion() != int64(AnyVersion) {
		t.Errorf("expected version: got %d want %d", srv.gotAppendReq.GetExpectedVersion(), AnyVersion)
	}
	if len(srv.gotAppendReq.GetEvents()) != 2 {
		t.Errorf("events count: %d", len(srv.gotAppendReq.GetEvents()))
	}
}

func TestExpectedVersionConstants(t *testing.T) {
	if NoStream != -1 || AnyVersion != -2 || StreamExists != -4 {
		t.Errorf("wire constants drifted: NoStream=%d AnyVersion=%d StreamExists=%d", NoStream, AnyVersion, StreamExists)
	}
}

func TestRead_Forward(t *testing.T) {
	srv := &fakeStreamServer{readFwdResp: &pb.ReadStreamResponse{
		Events: []*pb.RecordedEvent{
			{EventId: "e0", Version: 0},
			{EventId: "e1", Version: 1},
		},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Read(context.Background(), "s1", 0, 100)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 2 || got[0].Version != 0 || got[1].Version != 1 {
		t.Errorf("got %+v", got)
	}
	if srv.gotReadFwdReq.GetStartVersion() != 0 || srv.gotReadFwdReq.GetCount() != 100 {
		t.Errorf("req %+v", srv.gotReadFwdReq)
	}
}

func TestRead_Backward(t *testing.T) {
	srv := &fakeStreamServer{readBwdResp: &pb.ReadStreamResponse{
		Events: []*pb.RecordedEvent{
			{EventId: "e1", Version: 1},
			{EventId: "e0", Version: 0},
		},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.ReadBackward(context.Background(), "s1", 1, 100)
	if err != nil {
		t.Fatalf("ReadBackward: %v", err)
	}
	if len(got) != 2 || got[0].Version != 1 {
		t.Errorf("got %+v", got)
	}
}

func TestVersion(t *testing.T) {
	srv := &fakeStreamServer{versionResp: &pb.GetStreamVersionResponse{Version: 42}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Version(context.Background(), "s1")
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if got != 42 {
		t.Errorf("got %d", got)
	}
}

func TestList(t *testing.T) {
	srv := &fakeStreamServer{listResp: &pb.ListStreamsResponse{StreamIds: []string{"a", "b", "c"}}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("got %+v", got)
	}
}

func TestDelete(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeStreamServer{})
	defer cleanup()
	if err := c.Delete(context.Background(), "s1"); err != nil {
		t.Errorf("Delete: %v", err)
	}
}

func TestReadByEventTypes(t *testing.T) {
	srv := &fakeStreamServer{byTypesResp: &pb.ReadStreamResponse{
		Events: []*pb.RecordedEvent{{EventId: "e1", EventType: "t1"}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.ReadByEventTypes(context.Background(), []string{"t1", "t2"}, 500)
	if err != nil {
		t.Fatalf("ReadByEventTypes: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d want 1", len(got))
	}
	if !reflect.DeepEqual(srv.gotByTypesReq.GetEventTypes(), []string{"t1", "t2"}) {
		t.Errorf("req event types: %+v", srv.gotByTypesReq.GetEventTypes())
	}
	if srv.gotByTypesReq.GetBatchSize() != 500 {
		t.Errorf("batch: %d", srv.gotByTypesReq.GetBatchSize())
	}
}

func TestReadByTags_MatchMapping(t *testing.T) {
	cases := []struct {
		in   TagMatch
		want pb.TagMatch
	}{
		{TagMatchAny, pb.TagMatch_TAG_MATCH_ANY},
		{TagMatchAll, pb.TagMatch_TAG_MATCH_ALL},
		{TagMatch("garbage"), pb.TagMatch_TAG_MATCH_ANY}, // fallback
	}
	for _, tc := range cases {
		t.Run(string(tc.in), func(t *testing.T) {
			srv := &fakeStreamServer{byTagsResp: &pb.ReadStreamResponse{}}
			c, cleanup := newTestClient(t, srv)
			defer cleanup()
			if _, err := c.ReadByTags(context.Background(), []string{"tag1"}, tc.in, 100); err != nil {
				t.Fatalf("ReadByTags: %v", err)
			}
			if srv.gotByTagsReq.GetMatch() != tc.want {
				t.Errorf("match: got %v want %v", srv.gotByTagsReq.GetMatch(), tc.want)
			}
		})
	}
}

func TestReadAllGlobal(t *testing.T) {
	srv := &fakeStreamServer{globalResp: &pb.ReadStreamResponse{
		Events: []*pb.RecordedEvent{{EventId: "e1"}, {EventId: "e2"}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.ReadAllGlobal(context.Background(), 0, 100)
	if err != nil {
		t.Fatalf("ReadAllGlobal: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d want 2", len(got))
	}
}

func TestWatch_StreamDeliversAndCloses(t *testing.T) {
	srv := &fakeStreamServer{
		watchEvents: []*pb.RecordedEvent{
			{EventId: "e0", Version: 0},
			{EventId: "e1", Version: 1},
			{EventId: "e2", Version: 2},
		},
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	events, errs := c.Watch(context.Background(), "s1", 0, 0)
	var got []RecordedEvent
	for ev := range events {
		got = append(got, ev)
	}
	if err := <-errs; err != nil {
		t.Fatalf("errs: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d events want 3", len(got))
	}
	if got[0].EventID != "e0" || got[2].Version != 2 {
		t.Errorf("order/values: %+v", got)
	}
}

func TestWatch_ServerError(t *testing.T) {
	srv := &fakeStreamServer{watchErr: status.Error(codes.Internal, "boom")}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	events, errs := c.Watch(context.Background(), "s1", 0, 0)
	for range events {
	}
	if err := <-errs; err == nil {
		t.Fatalf("want server error")
	}
}

func TestEventTimestampMapping(t *testing.T) {
	srv := &fakeStreamServer{readFwdResp: &pb.ReadStreamResponse{
		Events: []*pb.RecordedEvent{{Timestamp: 1716200000000, EpochUs: 1716200000000000, PrevEventHash: []byte{0xfe, 0xed}}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Read(context.Background(), "s1", 0, 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !got[0].Timestamp.Equal(time.UnixMilli(1716200000000)) {
		t.Errorf("Timestamp: %v", got[0].Timestamp)
	}
	if !got[0].Epoch.Equal(time.UnixMicro(1716200000000000)) {
		t.Errorf("Epoch: %v", got[0].Epoch)
	}
	if !reflect.DeepEqual(got[0].PrevEventHash, []byte{0xfe, 0xed}) {
		t.Errorf("PrevEventHash: %v", got[0].PrevEventHash)
	}
}

func TestAllRPCErrors(t *testing.T) {
	srv := &fakeStreamServer{
		appendErr:   status.Error(codes.Internal, "x"),
		readFwdErr:  status.Error(codes.Internal, "x"),
		readBwdErr:  status.Error(codes.Internal, "x"),
		versionErr:  status.Error(codes.Internal, "x"),
		listErr:     status.Error(codes.Internal, "x"),
		deleteErr:   status.Error(codes.Internal, "x"),
		byTypesErr:  status.Error(codes.Internal, "x"),
		byTagsErr:   status.Error(codes.Internal, "x"),
		globalErr:   status.Error(codes.Internal, "x"),
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	ctx := context.Background()
	for name, fn := range map[string]func() error{
		"Append":   func() error { _, e := c.Append(ctx, "s", AnyVersion, nil); return e },
		"Read":     func() error { _, e := c.Read(ctx, "s", 0, 0); return e },
		"ReadBwd":  func() error { _, e := c.ReadBackward(ctx, "s", 0, 0); return e },
		"Version":  func() error { _, e := c.Version(ctx, "s"); return e },
		"List":     func() error { _, e := c.List(ctx); return e },
		"Delete":   func() error { return c.Delete(ctx, "s") },
		"ByTypes":  func() error { _, e := c.ReadByEventTypes(ctx, []string{"t"}, 0); return e },
		"ByTags":   func() error { _, e := c.ReadByTags(ctx, []string{"t"}, TagMatchAny, 0); return e },
		"Global":   func() error { _, e := c.ReadAllGlobal(ctx, 0, 0); return e },
	} {
		if err := fn(); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}
