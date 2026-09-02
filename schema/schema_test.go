package schema

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"

	pb "github.com/reckon-db-org/reckon-go/genproto/gatewayv1"
	"github.com/reckon-db-org/reckon-go/streams"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeSchemaServer struct {
	pb.UnimplementedSchemaServiceServer

	registerErr error

	unregisterErr error

	getResp *pb.GetSchemaResponse
	getErr  error

	listResp *pb.ListSchemasResponse
	listErr  error

	versionResp *pb.GetSchemaVersionResponse
	versionErr  error

	gotUpcastReq *pb.UpcastEventsRequest
	upcastResp   *pb.UpcastEventsResponse
	upcastErr    error
}

func (s *fakeSchemaServer) RegisterSchema(_ context.Context, _ *pb.RegisterSchemaRequest) (*pb.RegisterSchemaResponse, error) {
	if s.registerErr != nil {
		return nil, s.registerErr
	}
	return &pb.RegisterSchemaResponse{}, nil
}
func (s *fakeSchemaServer) UnregisterSchema(_ context.Context, _ *pb.UnregisterSchemaRequest) (*pb.UnregisterSchemaResponse, error) {
	if s.unregisterErr != nil {
		return nil, s.unregisterErr
	}
	return &pb.UnregisterSchemaResponse{}, nil
}
func (s *fakeSchemaServer) GetSchema(_ context.Context, _ *pb.GetSchemaRequest) (*pb.GetSchemaResponse, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.getResp, nil
}
func (s *fakeSchemaServer) ListSchemas(_ context.Context, _ *pb.ListSchemasRequest) (*pb.ListSchemasResponse, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.listResp, nil
}
func (s *fakeSchemaServer) GetSchemaVersion(_ context.Context, _ *pb.GetSchemaVersionRequest) (*pb.GetSchemaVersionResponse, error) {
	if s.versionErr != nil {
		return nil, s.versionErr
	}
	return s.versionResp, nil
}
func (s *fakeSchemaServer) UpcastEvents(_ context.Context, req *pb.UpcastEventsRequest) (*pb.UpcastEventsResponse, error) {
	s.gotUpcastReq = req
	if s.upcastErr != nil {
		return nil, s.upcastErr
	}
	return s.upcastResp, nil
}

func newTestClient(t *testing.T, srv *fakeSchemaServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterSchemaServiceServer(s, srv)
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

func TestRegister(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeSchemaServer{})
	defer cleanup()
	if err := c.Register(context.Background(), "evt_v1", []byte(`{}`)); err != nil {
		t.Errorf("Register: %v", err)
	}
}

func TestRegister_Error(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeSchemaServer{registerErr: status.Error(codes.InvalidArgument, "x")})
	defer cleanup()
	if err := c.Register(context.Background(), "evt_v1", []byte(`{}`)); err == nil {
		t.Fatalf("want error")
	}
}

func TestUnregister(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeSchemaServer{})
	defer cleanup()
	if err := c.Unregister(context.Background(), "evt_v1"); err != nil {
		t.Errorf("Unregister: %v", err)
	}
}

func TestGet(t *testing.T) {
	srv := &fakeSchemaServer{getResp: &pb.GetSchemaResponse{
		EventType: "evt_v1",
		Schema:    []byte(`{"type":"object"}`),
		Version:   7,
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Get(context.Background(), "evt_v1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := Definition{EventType: "evt_v1", Schema: []byte(`{"type":"object"}`), Version: 7}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestGet_NotRegistered(t *testing.T) {
	srv := &fakeSchemaServer{getResp: &pb.GetSchemaResponse{EventType: "evt_v1"}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Get(context.Background(), "evt_v1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Version != 0 || got.Schema != nil {
		t.Errorf("expected empty Schema/Version 0, got %+v", got)
	}
}

func TestList(t *testing.T) {
	srv := &fakeSchemaServer{listResp: &pb.ListSchemasResponse{
		Schemas: []*pb.GetSchemaResponse{
			{EventType: "a_v1", Version: 1},
			{EventType: "b_v2", Version: 2},
		},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[0].EventType != "a_v1" || got[1].Version != 2 {
		t.Errorf("got %+v", got)
	}
}

func TestVersion(t *testing.T) {
	srv := &fakeSchemaServer{versionResp: &pb.GetSchemaVersionResponse{Version: 9}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Version(context.Background(), "evt_v1")
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if got != 9 {
		t.Errorf("got %d want 9", got)
	}
}

func TestUpcast_RoundTripsEvents(t *testing.T) {
	srv := &fakeSchemaServer{upcastResp: &pb.UpcastEventsResponse{
		Events: []*pb.RecordedEvent{{EventId: "e1", EventType: "evt_v2", Version: 5}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	in := []streams.RecordedEvent{
		{EventID: "e1", EventType: "evt_v1", Version: 5, Data: []byte("payload"), Tags: []string{"t1"}},
	}
	got, err := c.Upcast(context.Background(), in)
	if err != nil {
		t.Fatalf("Upcast: %v", err)
	}
	if len(got) != 1 || got[0].EventType != "evt_v2" {
		t.Errorf("got %+v", got)
	}
	// Verify request was correctly marshalled from input
	if len(srv.gotUpcastReq.GetEvents()) != 1 || srv.gotUpcastReq.GetEvents()[0].GetEventId() != "e1" {
		t.Errorf("request events: %+v", srv.gotUpcastReq.GetEvents())
	}
	if string(srv.gotUpcastReq.GetEvents()[0].GetData()) != "payload" {
		t.Errorf("data not propagated: %q", srv.gotUpcastReq.GetEvents()[0].GetData())
	}
}
