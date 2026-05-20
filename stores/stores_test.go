package stores

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

// --- fake server ----------------------------------------------------

type fakeStoresServer struct {
	pb.UnimplementedStoresServiceServer

	listResp *pb.ListStoresResponse
	listErr  error

	getResp *pb.GetStoreResponse
	getErr  error

	watchEvents []*pb.StoreEvent // sequence to send, then EOF
	watchErr    error                  // returned mid-stream if non-nil
}

func (s *fakeStoresServer) ListStores(_ context.Context, _ *pb.ListStoresRequest) (*pb.ListStoresResponse, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.listResp, nil
}

func (s *fakeStoresServer) GetStore(_ context.Context, _ *pb.GetStoreRequest) (*pb.GetStoreResponse, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.getResp, nil
}

func (s *fakeStoresServer) WatchStores(_ *pb.WatchStoresRequest, srv pb.StoresService_WatchStoresServer) error {
	for _, ev := range s.watchEvents {
		if err := srv.Send(ev); err != nil {
			return err
		}
	}
	return s.watchErr
}

// --- test harness ---------------------------------------------------

func newTestClient(t *testing.T, srv *fakeStoresServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterStoresServiceServer(s, srv)
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
	return New(conn), func() { _ = conn.Close(); s.Stop() }
}

// --- tests ---------------------------------------------------------

func TestList(t *testing.T) {
	tests := []struct {
		name string
		resp *pb.ListStoresResponse
		want []Instance
	}{
		{"empty", &pb.ListStoresResponse{}, []Instance{}},
		{
			"two instances",
			&pb.ListStoresResponse{
				Instances: []*pb.StoreInstance{
					{StoreId: "alpha", Node: "n1@h", Mode: pb.StoreMode_STORE_MODE_SINGLE, DataDir: "/d", TimeoutMs: 5000, RegisteredAtUs: 1716120000000000},
					{StoreId: "beta", Node: "n2@h", Mode: pb.StoreMode_STORE_MODE_CLUSTER},
				},
			},
			[]Instance{
				{StoreID: "alpha", Node: "n1@h", Mode: ModeSingle, DataDir: "/d", Timeout: 5 * time.Second, RegisteredAt: time.UnixMicro(1716120000000000)},
				{StoreID: "beta", Node: "n2@h", Mode: ModeCluster, RegisteredAt: time.UnixMicro(0)},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, cleanup := newTestClient(t, &fakeStoresServer{listResp: tc.resp})
			defer cleanup()
			got, err := c.List(context.Background())
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestList_RPCError(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeStoresServer{listErr: status.Error(codes.Internal, "x")})
	defer cleanup()
	if _, err := c.List(context.Background()); err == nil {
		t.Fatalf("want error, got nil")
	}
}

func TestGet(t *testing.T) {
	srv := &fakeStoresServer{getResp: &pb.GetStoreResponse{
		Instances: []*pb.StoreInstance{
			{StoreId: "parksim_lot_store", Node: "parksim_lot@192.168.1.11", Mode: pb.StoreMode_STORE_MODE_SINGLE},
		},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Get(context.Background(), "parksim_lot_store")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != 1 || got[0].StoreID != "parksim_lot_store" || got[0].Mode != ModeSingle {
		t.Errorf("got %+v", got)
	}
}

func TestGet_RPCError(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeStoresServer{getErr: status.Error(codes.NotFound, "x")})
	defer cleanup()
	if _, err := c.Get(context.Background(), "x"); err == nil {
		t.Fatalf("want error, got nil")
	}
}

func TestModeFromProto(t *testing.T) {
	cases := map[pb.StoreMode]Mode{
		pb.StoreMode_STORE_MODE_SINGLE:      ModeSingle,
		pb.StoreMode_STORE_MODE_CLUSTER:     ModeCluster,
		pb.StoreMode_STORE_MODE_UNSPECIFIED: ModeUnspecified,
	}
	for in, want := range cases {
		if got := modeFromProto(in); got != want {
			t.Errorf("%v: got %v want %v", in, got, want)
		}
	}
}

func TestEventTypeFromProto(t *testing.T) {
	cases := map[pb.StoreEventType]EventType{
		pb.StoreEventType_STORE_EVENT_TYPE_ANNOUNCED:   EventAnnounced,
		pb.StoreEventType_STORE_EVENT_TYPE_RETIRED:     EventRetired,
		pb.StoreEventType_STORE_EVENT_TYPE_UNSPECIFIED: EventAnnounced, // default
	}
	for in, want := range cases {
		if got := eventTypeFromProto(in); got != want {
			t.Errorf("%v: got %v want %v", in, got, want)
		}
	}
}

func TestWatch_StreamDeliversAndCloses(t *testing.T) {
	srv := &fakeStoresServer{
		watchEvents: []*pb.StoreEvent{
			{Type: pb.StoreEventType_STORE_EVENT_TYPE_ANNOUNCED, Instance: &pb.StoreInstance{StoreId: "a"}, EventAtUs: 1},
			{Type: pb.StoreEventType_STORE_EVENT_TYPE_ANNOUNCED, Instance: &pb.StoreInstance{StoreId: "b"}, EventAtUs: 2},
			{Type: pb.StoreEventType_STORE_EVENT_TYPE_RETIRED, Instance: &pb.StoreInstance{StoreId: "a"}, EventAtUs: 3},
		},
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	events, errs := c.Watch(context.Background())
	var got []Event
	for ev := range events {
		got = append(got, ev)
	}
	if err := <-errs; err != nil {
		t.Fatalf("errs: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d events, want 3", len(got))
	}
	if got[0].Type != EventAnnounced || got[0].Instance.StoreID != "a" {
		t.Errorf("event 0: %+v", got[0])
	}
	if got[2].Type != EventRetired {
		t.Errorf("event 2: %+v", got[2])
	}
}

func TestWatch_ServerError(t *testing.T) {
	srv := &fakeStoresServer{watchErr: status.Error(codes.Internal, "boom")}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	events, errs := c.Watch(context.Background())
	for range events {
	}
	if err := <-errs; err == nil {
		t.Fatalf("want server error, got nil")
	}
}

func TestWatch_ContextCancelled(t *testing.T) {
	srv := &fakeStoresServer{
		watchEvents: []*pb.StoreEvent{
			{Type: pb.StoreEventType_STORE_EVENT_TYPE_ANNOUNCED, Instance: &pb.StoreInstance{StoreId: "a"}},
		},
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	events, errs := c.Watch(ctx)
	cancel()
	// Drain to avoid leaking goroutine; tolerate either error or
	// clean close depending on timing.
	for range events {
	}
	<-errs
}
