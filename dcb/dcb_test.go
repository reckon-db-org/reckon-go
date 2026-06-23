package dcb

import (
	"context"
	"net"
	"reflect"
	"testing"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

//
// Filter-algebra tests (pure Go, no gRPC)
//

func TestMatchAnyToProto(t *testing.T) {
	got := MatchAny("a", "b").toProto()
	want := &pb.TagFilter{Kind: &pb.TagFilter_MatchAny{
		MatchAny: &pb.TagList{Tags: []string{"a", "b"}},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MatchAny: got %+v want %+v", got, want)
	}
}

func TestMatchAllToProto(t *testing.T) {
	got := MatchAll("x").toProto()
	want := &pb.TagFilter{Kind: &pb.TagFilter_MatchAll{
		MatchAll: &pb.TagList{Tags: []string{"x"}},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MatchAll: got %+v want %+v", got, want)
	}
}

func TestEventTypeToProto(t *testing.T) {
	got := EventType("inventory_reserved_v1").toProto()
	want := &pb.TagFilter{Kind: &pb.TagFilter_EventTypeMatch{
		EventTypeMatch: "inventory_reserved_v1",
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("EventType: got %+v want %+v", got, want)
	}
}

func TestAndWithEventTypeToProto(t *testing.T) {
	f := And(EventType("order_placed_v1"), MatchAny("customer:42"))
	got := f.toProto()
	conj, ok := got.GetKind().(*pb.TagFilter_Conjunction)
	if !ok {
		t.Fatalf("expected Conjunction at root, got %T", got.GetKind())
	}
	if n := len(conj.Conjunction.GetFilters()); n != 2 {
		t.Fatalf("expected 2 sub-filters, got %d", n)
	}
	_, ok = conj.Conjunction.Filters[0].GetKind().(*pb.TagFilter_EventTypeMatch)
	if !ok {
		t.Fatalf("sub[0]: expected EventTypeMatch, got %T",
			conj.Conjunction.Filters[0].GetKind())
	}
}

func TestAndOrNestingToProto(t *testing.T) {
	// {or_, [{and_, [match_any [r], match_all [g]]}, match_any [c]]}
	f := Or(
		And(MatchAny("r"), MatchAll("g")),
		MatchAny("c"),
	)
	got := f.toProto()

	// Walk the oneof to confirm structure.
	disj, ok := got.GetKind().(*pb.TagFilter_Disjunction)
	if !ok {
		t.Fatalf("expected Disjunction at root, got %T", got.GetKind())
	}
	if n := len(disj.Disjunction.GetFilters()); n != 2 {
		t.Fatalf("expected 2 sub-filters, got %d", n)
	}

	// First sub: conjunction.
	conj, ok := disj.Disjunction.Filters[0].GetKind().(*pb.TagFilter_Conjunction)
	if !ok {
		t.Fatalf("expected Conjunction at sub[0], got %T",
			disj.Disjunction.Filters[0].GetKind())
	}
	if n := len(conj.Conjunction.GetFilters()); n != 2 {
		t.Fatalf("expected 2 inner sub-filters, got %d", n)
	}

	// Second sub: match_any [c].
	any, ok := disj.Disjunction.Filters[1].GetKind().(*pb.TagFilter_MatchAny)
	if !ok {
		t.Fatalf("expected MatchAny at sub[1], got %T",
			disj.Disjunction.Filters[1].GetKind())
	}
	if got, want := any.MatchAny.GetTags(), []string{"c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sub[1] tags: got %v want %v", got, want)
	}
}

//
// gRPC fake-server tests
//

type fakeDcbServer struct {
	pb.UnimplementedDcbServiceServer

	gotAppend   *pb.AppendIfNoTagMatchesRequest
	appendResp  *pb.AppendIfNoTagMatchesResponse
	appendErr   error

	gotRead    *pb.ReadDcbContextRequest
	readResp   *pb.ReadDcbContextResponse
	readErr    error
}

func (s *fakeDcbServer) AppendIfNoTagMatches(_ context.Context, req *pb.AppendIfNoTagMatchesRequest) (*pb.AppendIfNoTagMatchesResponse, error) {
	s.gotAppend = req
	return s.appendResp, s.appendErr
}

func (s *fakeDcbServer) ReadDcbContext(_ context.Context, req *pb.ReadDcbContextRequest) (*pb.ReadDcbContextResponse, error) {
	s.gotRead = req
	return s.readResp, s.readErr
}

func newTestClientWithServer(t *testing.T, srv pb.DcbServiceServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	pb.RegisterDcbServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.DialContext(context.Background())
	}
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := New(conn, "test_store")
	return c, func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	}
}

func newTestClient(t *testing.T, srv *fakeDcbServer) (*Client, func()) {
	t.Helper()
	return newTestClientWithServer(t, srv)
}

func TestAppendCommitted(t *testing.T) {
	srv := &fakeDcbServer{
		appendResp: &pb.AppendIfNoTagMatchesResponse{
			Result: &pb.AppendIfNoTagMatchesResponse_Committed{
				Committed: &pb.Committed{LastSeq: 42},
			},
		},
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	committed, conflict, err := c.Append(context.Background(),
		MatchAny("slot:7"),
		SeqCutoffSawNothing,
		[]ProposedEvent{{EventType: "reserved_v1", Tags: []string{"slot:7"}}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if conflict != nil {
		t.Fatalf("expected committed, got conflict: %+v", conflict)
	}
	if committed == nil || committed.LastSeq != 42 {
		t.Fatalf("LastSeq: got %+v want LastSeq=42", committed)
	}

	if srv.gotAppend.GetStoreId() != "test_store" {
		t.Errorf("StoreId: %q", srv.gotAppend.GetStoreId())
	}
	if srv.gotAppend.GetSeqCutoff() != SeqCutoffSawNothing {
		t.Errorf("SeqCutoff: %d", srv.gotAppend.GetSeqCutoff())
	}
}

func TestAppendConflict(t *testing.T) {
	srv := &fakeDcbServer{
		appendResp: &pb.AppendIfNoTagMatchesResponse{
			Result: &pb.AppendIfNoTagMatchesResponse_Conflict{
				Conflict: &pb.Conflict{MaxSeq: 99},
			},
		},
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	committed, conflict, err := c.Append(context.Background(),
		MatchAny("slot:1"),
		5,
		[]ProposedEvent{{EventType: "x_v1"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if committed != nil {
		t.Fatalf("expected conflict, got committed: %+v", committed)
	}
	if conflict == nil || conflict.MaxSeq != 99 {
		t.Fatalf("MaxSeq: got %+v want MaxSeq=99", conflict)
	}
}

func TestAppendNilFilter(t *testing.T) {
	srv := &fakeDcbServer{}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	_, _, err := c.Append(context.Background(), nil, -1, nil)
	if err == nil {
		t.Fatalf("expected error for nil filter")
	}
	if srv.gotAppend != nil {
		t.Fatalf("nil filter should short-circuit before RPC; got %+v",
			srv.gotAppend)
	}
}

func TestReadHappyPath(t *testing.T) {
	srv := &fakeDcbServer{
		readResp: &pb.ReadDcbContextResponse{
			Events: []*pb.RecordedEvent{
				{EventId: "ev-1", EventType: "reserved_v1",
					StreamId: "_dcb", Version: 5,
					Tags: []string{"slot:1"}},
				{EventId: "ev-2", EventType: "reserved_v1",
					StreamId: "_dcb", Version: 11,
					Tags: []string{"slot:1"}},
			},
			MaxSeq: 11,
		},
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	res, err := c.Read(context.Background(), MatchAny("slot:1"), 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(res.Events) != 2 {
		t.Fatalf("events len: got %d want 2", len(res.Events))
	}
	if res.MaxSeq != 11 {
		t.Errorf("MaxSeq: got %d want 11", res.MaxSeq)
	}
	if res.Events[0].EventID != "ev-1" {
		t.Errorf("Event[0].EventID: %q", res.Events[0].EventID)
	}
	if srv.gotRead.GetBatchSize() != 100 {
		t.Errorf("BatchSize: %d", srv.gotRead.GetBatchSize())
	}
}

func TestReadEmpty(t *testing.T) {
	srv := &fakeDcbServer{
		readResp: &pb.ReadDcbContextResponse{
			Events: nil,
			MaxSeq: -1,
		},
	}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	res, err := c.Read(context.Background(), MatchAny("slot:99"), 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(res.Events) != 0 {
		t.Fatalf("events: %d", len(res.Events))
	}
	if res.MaxSeq != SeqCutoffSawNothing {
		t.Errorf("MaxSeq: got %d want %d", res.MaxSeq, SeqCutoffSawNothing)
	}
}

func TestDecisionLoop(t *testing.T) {
	// Walk the canonical pattern: read context -> append -> conflict
	// -> re-read -> append -> commit.
	srv := &fakeDcbServer{}
	c, cleanup := newTestClient(t, srv)
	defer cleanup()

	// First read: stale context at seq 5.
	srv.readResp = &pb.ReadDcbContextResponse{
		Events: []*pb.RecordedEvent{
			{EventId: "ev-1", StreamId: "_dcb", Version: 5},
		},
		MaxSeq: 5,
	}
	res1, err := c.Read(context.Background(), MatchAny("slot:42"), 100)
	if err != nil {
		t.Fatalf("read1: %v", err)
	}
	if res1.MaxSeq != 5 {
		t.Fatalf("res1.MaxSeq: %d", res1.MaxSeq)
	}

	// First append: server advanced to seq 7; conflict.
	srv.appendResp = &pb.AppendIfNoTagMatchesResponse{
		Result: &pb.AppendIfNoTagMatchesResponse_Conflict{
			Conflict: &pb.Conflict{MaxSeq: 7},
		},
	}
	_, conflict, err := c.Append(context.Background(),
		MatchAny("slot:42"), res1.MaxSeq, []ProposedEvent{{EventType: "r_v1"}})
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	if conflict == nil || conflict.MaxSeq != 7 {
		t.Fatalf("append1 expected conflict@7, got %+v", conflict)
	}

	// Second append at seq 7: commit at seq 8.
	srv.appendResp = &pb.AppendIfNoTagMatchesResponse{
		Result: &pb.AppendIfNoTagMatchesResponse_Committed{
			Committed: &pb.Committed{LastSeq: 8},
		},
	}
	committed, conflict, err := c.Append(context.Background(),
		MatchAny("slot:42"), int64(conflict.MaxSeq),
		[]ProposedEvent{{EventType: "r_v1"}})
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	if conflict != nil {
		t.Fatalf("append2 unexpected conflict: %+v", conflict)
	}
	if committed == nil || committed.LastSeq != 8 {
		t.Fatalf("append2 expected commit@8, got %+v", committed)
	}
}
