package dcb

import (
	"context"
	"testing"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
)

// fakeCccServer extends fakeDcbServer with CCC payload RPC handlers.
type fakeCccServer struct {
	pb.UnimplementedDcbServiceServer

	gotByPayload     *pb.CccReadByPayloadRequest
	byPayloadResp    *pb.CccReadByPayloadResponse
	byPayloadErr     error

	gotByPayloadHash *pb.CccReadByPayloadHashRequest
	byHashResp       *pb.CccReadByPayloadHashResponse
	byHashErr        error
}

func (s *fakeCccServer) CccReadByPayload(_ context.Context, req *pb.CccReadByPayloadRequest) (*pb.CccReadByPayloadResponse, error) {
	s.gotByPayload = req
	return s.byPayloadResp, s.byPayloadErr
}

func (s *fakeCccServer) CccReadByPayloadHash(_ context.Context, req *pb.CccReadByPayloadHashRequest) (*pb.CccReadByPayloadHashResponse, error) {
	s.gotByPayloadHash = req
	return s.byHashResp, s.byHashErr
}

func newCccTestClient(t *testing.T, srv *fakeCccServer) (*Client, func()) {
	t.Helper()
	return newTestClientWithServer(t, srv)
}

func TestCccReadByPayloadHappyPath(t *testing.T) {
	srv := &fakeCccServer{
		byPayloadResp: &pb.CccReadByPayloadResponse{
			Events: []*pb.RecordedEvent{
				{EventId: "ev-1", EventType: "credit_reserved_v1", StreamId: "_dcb", Version: 3},
				{EventId: "ev-2", EventType: "credit_released_v1", StreamId: "_dcb", Version: 7},
			},
		},
	}
	c, cleanup := newCccTestClient(t, srv)
	defer cleanup()

	events, err := c.CccReadByPayload(context.Background(), "account_id", "acc-42", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].EventID != "ev-1" {
		t.Errorf("events[0].EventID: got %q want %q", events[0].EventID, "ev-1")
	}
	if events[1].EventType != "credit_released_v1" {
		t.Errorf("events[1].EventType: got %q want %q", events[1].EventType, "credit_released_v1")
	}

	if srv.gotByPayload.GetStoreId() != "test_store" {
		t.Errorf("StoreId: %q", srv.gotByPayload.GetStoreId())
	}
	if srv.gotByPayload.GetKey() != "account_id" {
		t.Errorf("Key: %q", srv.gotByPayload.GetKey())
	}
	if srv.gotByPayload.GetValue() != "acc-42" {
		t.Errorf("Value: %q", srv.gotByPayload.GetValue())
	}
	if srv.gotByPayload.GetBatchSize() != 100 {
		t.Errorf("BatchSize: %d", srv.gotByPayload.GetBatchSize())
	}
}

func TestCccReadByPayloadEmpty(t *testing.T) {
	srv := &fakeCccServer{
		byPayloadResp: &pb.CccReadByPayloadResponse{Events: nil},
	}
	c, cleanup := newCccTestClient(t, srv)
	defer cleanup()

	events, err := c.CccReadByPayload(context.Background(), "account_id", "missing", 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}

func TestCccReadByPayloadHashHappyPath(t *testing.T) {
	srv := &fakeCccServer{
		byHashResp: &pb.CccReadByPayloadHashResponse{
			Events: []*pb.RecordedEvent{
				{EventId: "ev-5", EventType: "seat_reserved_v1", StreamId: "_dcb", Version: 12},
			},
		},
	}
	c, cleanup := newCccTestClient(t, srv)
	defer cleanup()

	keys := []string{"flight_id", "seat_no"}
	values := []string{"FL-001", "14A"}
	events, err := c.CccReadByPayloadHash(context.Background(), keys, values, 200)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].EventID != "ev-5" {
		t.Errorf("events[0].EventID: got %q want %q", events[0].EventID, "ev-5")
	}

	if got := srv.gotByPayloadHash.GetKeys(); len(got) != 2 || got[0] != "flight_id" || got[1] != "seat_no" {
		t.Errorf("Keys: %v", got)
	}
	if got := srv.gotByPayloadHash.GetValues(); len(got) != 2 || got[0] != "FL-001" || got[1] != "14A" {
		t.Errorf("Values: %v", got)
	}
	if srv.gotByPayloadHash.GetBatchSize() != 200 {
		t.Errorf("BatchSize: %d", srv.gotByPayloadHash.GetBatchSize())
	}
}

func TestCccReadByPayloadHashEmpty(t *testing.T) {
	srv := &fakeCccServer{
		byHashResp: &pb.CccReadByPayloadHashResponse{Events: nil},
	}
	c, cleanup := newCccTestClient(t, srv)
	defer cleanup()

	events, err := c.CccReadByPayloadHash(context.Background(), []string{"tenant_id"}, []string{"nobody"}, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}
