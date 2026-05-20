package causation

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

type fakeCausationServer struct {
	pb.UnimplementedCausationServiceServer

	effectsResp *pb.CausationResponse
	effectsErr  error

	causeResp *pb.SingleEventResponse
	causeErr  error

	chainResp *pb.CausationResponse
	chainErr  error

	correlatedResp *pb.CausationResponse
	correlatedErr  error

	graphResp *pb.CausationGraphResponse
	graphErr  error
}

func (s *fakeCausationServer) GetEffects(_ context.Context, _ *pb.CausationRequest) (*pb.CausationResponse, error) {
	if s.effectsErr != nil {
		return nil, s.effectsErr
	}
	return s.effectsResp, nil
}

func (s *fakeCausationServer) GetCause(_ context.Context, _ *pb.CausationRequest) (*pb.SingleEventResponse, error) {
	if s.causeErr != nil {
		return nil, s.causeErr
	}
	return s.causeResp, nil
}

func (s *fakeCausationServer) GetCausationChain(_ context.Context, _ *pb.CausationRequest) (*pb.CausationResponse, error) {
	if s.chainErr != nil {
		return nil, s.chainErr
	}
	return s.chainResp, nil
}

func (s *fakeCausationServer) GetCorrelated(_ context.Context, _ *pb.CorrelationRequest) (*pb.CausationResponse, error) {
	if s.correlatedErr != nil {
		return nil, s.correlatedErr
	}
	return s.correlatedResp, nil
}

func (s *fakeCausationServer) BuildCausationGraph(_ context.Context, _ *pb.CausationRequest) (*pb.CausationGraphResponse, error) {
	if s.graphErr != nil {
		return nil, s.graphErr
	}
	return s.graphResp, nil
}

func newTestClient(t *testing.T, srv *fakeCausationServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	pb.RegisterCausationServiceServer(s, srv)
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

func TestEffects(t *testing.T) {
	srv := &fakeCausationServer{effectsResp: &pb.CausationResponse{
		Events: []*pb.RecordedEvent{{EventId: "e1"}, {EventId: "e2"}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Effects(context.Background(), "root")
	if err != nil {
		t.Fatalf("Effects: %v", err)
	}
	if len(got) != 2 || got[0].EventID != "e1" {
		t.Errorf("got %+v", got)
	}
}

func TestCause_Found(t *testing.T) {
	srv := &fakeCausationServer{causeResp: &pb.SingleEventResponse{
		Event: &pb.RecordedEvent{EventId: "parent"},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Cause(context.Background(), "child")
	if err != nil {
		t.Fatalf("Cause: %v", err)
	}
	if got.EventID != "parent" {
		t.Errorf("got %+v", got)
	}
}

func TestCause_NoCause(t *testing.T) {
	srv := &fakeCausationServer{causeResp: &pb.SingleEventResponse{Event: nil}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Cause(context.Background(), "root")
	if err != nil {
		t.Fatalf("Cause: %v", err)
	}
	if got.EventID != "" {
		t.Errorf("expected zero RecordedEvent, got %+v", got)
	}
}

func TestChain(t *testing.T) {
	srv := &fakeCausationServer{chainResp: &pb.CausationResponse{
		Events: []*pb.RecordedEvent{{EventId: "root"}, {EventId: "mid"}, {EventId: "leaf"}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Chain(context.Background(), "leaf")
	if err != nil {
		t.Fatalf("Chain: %v", err)
	}
	if len(got) != 3 || got[0].EventID != "root" {
		t.Errorf("got %+v", got)
	}
}

func TestCorrelated(t *testing.T) {
	srv := &fakeCausationServer{correlatedResp: &pb.CausationResponse{
		Events: []*pb.RecordedEvent{{EventId: "x"}, {EventId: "y"}},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Correlated(context.Background(), "corr-123")
	if err != nil {
		t.Fatalf("Correlated: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %+v", got)
	}
}

func TestGraph(t *testing.T) {
	// root → [a, b]; b → [c]
	srv := &fakeCausationServer{graphResp: &pb.CausationGraphResponse{
		Root: &pb.CausationGraphNode{
			Event: &pb.RecordedEvent{EventId: "root"},
			Children: []*pb.CausationGraphNode{
				{Event: &pb.RecordedEvent{EventId: "a"}},
				{
					Event: &pb.RecordedEvent{EventId: "b"},
					Children: []*pb.CausationGraphNode{
						{Event: &pb.RecordedEvent{EventId: "c"}},
					},
				},
			},
		},
	}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Graph(context.Background(), "root")
	if err != nil {
		t.Fatalf("Graph: %v", err)
	}
	if got.Event.EventID != "root" {
		t.Errorf("root: %+v", got.Event)
	}
	if len(got.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(got.Children))
	}
	if got.Children[1].Event.EventID != "b" || len(got.Children[1].Children) != 1 || got.Children[1].Children[0].Event.EventID != "c" {
		t.Errorf("tree shape: %+v", got)
	}
}

func TestGraph_NilRoot(t *testing.T) {
	srv := &fakeCausationServer{graphResp: &pb.CausationGraphResponse{Root: nil}}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	got, err := c.Graph(context.Background(), "x")
	if err != nil {
		t.Fatalf("Graph: %v", err)
	}
	if got.Event.EventID != "" || len(got.Children) != 0 {
		t.Errorf("expected zero GraphNode, got %+v", got)
	}
}

func TestErrors(t *testing.T) {
	srv := &fakeCausationServer{
		effectsErr:    status.Error(codes.Internal, "x"),
		causeErr:      status.Error(codes.Internal, "x"),
		chainErr:      status.Error(codes.Internal, "x"),
		correlatedErr: status.Error(codes.Internal, "x"),
		graphErr:      status.Error(codes.Internal, "x"),
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()
	ctx := context.Background()
	if _, err := c.Effects(ctx, "x"); err == nil {
		t.Error("Effects: want error")
	}
	if _, err := c.Cause(ctx, "x"); err == nil {
		t.Error("Cause: want error")
	}
	if _, err := c.Chain(ctx, "x"); err == nil {
		t.Error("Chain: want error")
	}
	if _, err := c.Correlated(ctx, "x"); err == nil {
		t.Error("Correlated: want error")
	}
	if _, err := c.Graph(ctx, "x"); err == nil {
		t.Error("Graph: want error")
	}
}
