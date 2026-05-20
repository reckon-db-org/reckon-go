package subscriptions

import (
	"context"
	"errors"
	"net"
	"testing"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeSubServer struct {
	pb.UnimplementedSubscriptionServiceServer

	gotCreateReq *pb.CreateSubscriptionRequest
	createResp   *pb.CreateSubscriptionResponse
	createErr    error

	removeErr error

	listResp *pb.ListSubscriptionsResponse
	listErr  error

	getResp *pb.SubscriptionInfo
	getErr  error

	lagResp *pb.GetSubscriptionLagResponse
	lagErr  error

	gotAckReq *pb.AckEventRequest
	ackErr    error

	subscribeDeliveries []*pb.SubscriptionEvent
	subscribeErr        error
}

func (s *fakeSubServer) CreateSubscription(_ context.Context, req *pb.CreateSubscriptionRequest) (*pb.CreateSubscriptionResponse, error) {
	s.gotCreateReq = req
	if s.createErr != nil {
		return nil, s.createErr
	}
	return s.createResp, nil
}
func (s *fakeSubServer) RemoveSubscription(_ context.Context, _ *pb.RemoveSubscriptionRequest) (*pb.RemoveSubscriptionResponse, error) {
	if s.removeErr != nil {
		return nil, s.removeErr
	}
	return &pb.RemoveSubscriptionResponse{}, nil
}
func (s *fakeSubServer) ListSubscriptions(_ context.Context, _ *pb.ListSubscriptionsRequest) (*pb.ListSubscriptionsResponse, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.listResp, nil
}
func (s *fakeSubServer) GetSubscription(_ context.Context, _ *pb.GetSubscriptionRequest) (*pb.SubscriptionInfo, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.getResp, nil
}
func (s *fakeSubServer) GetSubscriptionLag(_ context.Context, _ *pb.GetSubscriptionLagRequest) (*pb.GetSubscriptionLagResponse, error) {
	if s.lagErr != nil {
		return nil, s.lagErr
	}
	return s.lagResp, nil
}
func (s *fakeSubServer) AckEvent(_ context.Context, req *pb.AckEventRequest) (*pb.AckEventResponse, error) {
	s.gotAckReq = req
	if s.ackErr != nil {
		return nil, s.ackErr
	}
	return &pb.AckEventResponse{}, nil
}
func (s *fakeSubServer) Subscribe(_ *pb.SubscribeRequest, srv pb.SubscriptionService_SubscribeServer) error {
	for _, d := range s.subscribeDeliveries {
		if err := srv.Send(d); err != nil {
			return err
		}
	}
	return s.subscribeErr
}

func newTestClient(t *testing.T, srv *fakeSubServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterSubscriptionServiceServer(s, srv)
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

func TestTypeRoundTrip(t *testing.T) {
	cases := []Type{TypeStream, TypeEventType, TypeEventPattern, TypeEventPayload, TypeTags, TypeUnspecified}
	for _, tc := range cases {
		got := typeFromProto(typeToProto(tc))
		if got != tc {
			t.Errorf("%v: roundtrip got %v", tc, got)
		}
	}
}

func TestTypeFromProto_UnknownDefaults(t *testing.T) {
	if got := typeFromProto(pb.SubscriptionType(99)); got != TypeUnspecified {
		t.Errorf("unknown should map to TypeUnspecified, got %v", got)
	}
}

func TestCreate(t *testing.T) {
	srv := &fakeSubServer{createResp: &pb.CreateSubscriptionResponse{SubscriptionId: "sub-1"}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	id, err := c.Create(context.Background(), Spec{
		Type: TypeStream, Selector: "users-42", Name: "billing", StartFrom: 100, PoolSize: 4,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id != "sub-1" {
		t.Errorf("id: got %q", id)
	}
	if srv.gotCreateReq.GetType() != pb.SubscriptionType_SUBSCRIPTION_TYPE_STREAM ||
		srv.gotCreateReq.GetSelector() != "users-42" ||
		srv.gotCreateReq.GetSubscriptionName() != "billing" ||
		srv.gotCreateReq.GetStartFrom() != 100 ||
		srv.gotCreateReq.GetPoolSize() != 4 {
		t.Errorf("request: %+v", srv.gotCreateReq)
	}
}

func TestRemove(t *testing.T) {
	c, cleanup := newTestClient(t, &fakeSubServer{})
	defer cleanup()
	if err := c.Remove(context.Background(), Spec{Type: TypeStream, Selector: "users-42", Name: "billing"}); err != nil {
		t.Errorf("Remove: %v", err)
	}
}

func TestList(t *testing.T) {
	srv := &fakeSubServer{listResp: &pb.ListSubscriptionsResponse{
		Subscriptions: []*pb.SubscriptionInfo{
			{Id: "1", SubscriptionName: "a", Type: pb.SubscriptionType_SUBSCRIPTION_TYPE_STREAM, Selector: "s1", Checkpoint: 10, CreatedAt: 1716200000000},
			{Id: "2", SubscriptionName: "b", Type: pb.SubscriptionType_SUBSCRIPTION_TYPE_TAGS, Selector: "t1,t2", Checkpoint: 5},
		},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[0].Name != "a" || got[0].Type != TypeStream || got[0].Checkpoint != 10 {
		t.Errorf("got %+v", got)
	}
	if got[1].Type != TypeTags {
		t.Errorf("type: %v", got[1].Type)
	}
}

func TestGet(t *testing.T) {
	srv := &fakeSubServer{getResp: &pb.SubscriptionInfo{
		Id: "1", SubscriptionName: "billing", Type: pb.SubscriptionType_SUBSCRIPTION_TYPE_STREAM, Checkpoint: 99,
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Get(context.Background(), "billing")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "billing" || got.Checkpoint != 99 {
		t.Errorf("got %+v", got)
	}
}

func TestLag(t *testing.T) {
	srv := &fakeSubServer{lagResp: &pb.GetSubscriptionLagResponse{
		Lag: 50, CurrentCheckpoint: 100, LatestVersion: 150,
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Lag(context.Background(), "billing")
	if err != nil {
		t.Fatalf("Lag: %v", err)
	}
	want := Lag{Lag: 50, CurrentCheckpoint: 100, LatestVersion: 150}
	if got != want {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestAck(t *testing.T) {
	srv := &fakeSubServer{}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	if err := c.Ack(context.Background(), "users-42", "billing", 100); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if srv.gotAckReq.GetStreamId() != "users-42" ||
		srv.gotAckReq.GetSubscriptionName() != "billing" ||
		srv.gotAckReq.GetEventNumber() != 100 {
		t.Errorf("ack request: %+v", srv.gotAckReq)
	}
}

func TestSubscribe_DeliversAndCloses(t *testing.T) {
	srv := &fakeSubServer{
		subscribeDeliveries: []*pb.SubscriptionEvent{
			{Event: &pb.RecordedEvent{EventId: "e1", Version: 1}, Checkpoint: 1},
			{Event: &pb.RecordedEvent{EventId: "e2", Version: 2}, Checkpoint: 2},
		},
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	deliveries, errs := c.Subscribe(context.Background(), Spec{
		Type: TypeStream, Selector: "users-42", Name: "billing",
	})
	var got []Delivery
	for d := range deliveries {
		got = append(got, d)
	}
	if err := <-errs; err != nil {
		t.Fatalf("errs: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d deliveries want 2", len(got))
	}
	if got[0].Event.EventID != "e1" || got[0].Checkpoint != 1 || got[1].Checkpoint != 2 {
		t.Errorf("got %+v", got)
	}
}

func TestSubscribe_ServerError(t *testing.T) {
	srv := &fakeSubServer{subscribeErr: status.Error(codes.Internal, "boom")}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	deliveries, errs := c.Subscribe(context.Background(), Spec{})
	for range deliveries {
	}
	if err := <-errs; err == nil {
		t.Fatalf("want server error")
	}
}

func TestAllRPCErrors(t *testing.T) {
	srv := &fakeSubServer{
		createErr: status.Error(codes.Internal, "x"),
		removeErr: status.Error(codes.Internal, "x"),
		listErr:   status.Error(codes.Internal, "x"),
		getErr:    status.Error(codes.Internal, "x"),
		lagErr:    status.Error(codes.Internal, "x"),
		ackErr:    status.Error(codes.Internal, "x"),
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	ctx := context.Background()
	if _, err := c.Create(ctx, Spec{}); err == nil {
		t.Error("Create: want error")
	}
	if err := c.Remove(ctx, Spec{}); err == nil {
		t.Error("Remove: want error")
	}
	if _, err := c.List(ctx); err == nil {
		t.Error("List: want error")
	}
	if _, err := c.Get(ctx, "x"); err == nil {
		t.Error("Get: want error")
	}
	if _, err := c.Lag(ctx, "x"); err == nil {
		t.Error("Lag: want error")
	}
	if err := c.Ack(ctx, "s", "n", 0); err == nil {
		t.Error("Ack: want error")
	}
}
