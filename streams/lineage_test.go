package streams

import (
	"context"
	"encoding/json"
	"testing"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
)

// The convenience readers must route to ReadByMetadata with the reserved keys.
func TestLineageReaders_UseReservedKeys(t *testing.T) {
	cases := []struct {
		name    string
		call    func(c *Client) error
		wantKey string
		wantVal string
	}{
		{"effects", func(c *Client) error {
			_, e := c.ReadEffects(context.Background(), "evt-7", 100)
			return e
		}, CausationIDKey, "evt-7"},
		{"correlated", func(c *Client) error {
			_, e := c.ReadCorrelated(context.Background(), "saga-1", 100)
			return e
		}, CorrelationIDKey, "saga-1"},
		{"conversation", func(c *Client) error {
			_, e := c.ReadConversation(context.Background(), "order-9", 100)
			return e
		}, ConversationIDKey, "order-9"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fakeStreamServer{byMetaResp: &pb.ReadStreamResponse{}}
			c, cleanup := newTestClient(t, srv)
			defer cleanup()
			if err := tc.call(c); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if srv.gotByMetaReq.GetKey() != tc.wantKey {
				t.Errorf("key = %q, want %q", srv.gotByMetaReq.GetKey(), tc.wantKey)
			}
			if srv.gotByMetaReq.GetValue() != tc.wantVal {
				t.Errorf("value = %q, want %q", srv.gotByMetaReq.GetValue(), tc.wantVal)
			}
		})
	}
}

// The reserved key constants must match the reckon-proto contract exactly.
func TestReservedKeyConstants(t *testing.T) {
	if CausationIDKey != "causation_id" || CorrelationIDKey != "correlation_id" || ConversationIDKey != "conversation_id" {
		t.Errorf("reserved keys drifted: %q %q %q", CausationIDKey, CorrelationIDKey, ConversationIDKey)
	}
}

func TestWithLineage(t *testing.T) {
	m := WithLineage(map[string]any{"tenant": "acme"}, "cause-1", "corr-1")
	if m[CausationIDKey] != "cause-1" || m[CorrelationIDKey] != "corr-1" || m["tenant"] != "acme" {
		t.Fatalf("WithLineage merged wrong: %+v", m)
	}
	// Empty values are skipped, and nil base map is tolerated.
	m2 := WithLineage(nil, "", "corr-only")
	if _, ok := m2[CausationIDKey]; ok {
		t.Errorf("empty causation should be skipped: %+v", m2)
	}
	if m2[CorrelationIDKey] != "corr-only" {
		t.Errorf("correlation not set: %+v", m2)
	}
	// Result marshals to JSON with the reserved keys (wire shape).
	b, err := json.Marshal(WithLineage(nil, "c1", "r1"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(b, &back); err != nil || back["causation_id"] != "c1" {
		t.Errorf("json round-trip wrong: %s", b)
	}
}
