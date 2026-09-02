package encode

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/reckon-db-org/reckon-go/stores"
	"github.com/reckon-db-org/reckon-go/streams"
)

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

var ts = time.Date(2026, 5, 20, 14, 3, 11, 123000000, time.UTC)

func TestEventAutoDecodesJSON(t *testing.T) {
	e := streams.RecordedEvent{
		EventID:         "evt-1",
		EventType:       "user_registered_v1",
		StreamID:        "user-123",
		Version:         7,
		Data:            []byte(`{"name":"Ada"}`),
		DataContentType: "application/json",
		Tags:            []string{"user"},
		Timestamp:       ts,
	}
	got := mustJSON(t, Event(e, Auto))
	// decoded JSON object present and inlined (not a quoted string)
	want := `"data":{"name":"Ada"}`
	if !contains(got, want) {
		t.Errorf("auto data not inlined as JSON\n got: %s\nwant substr: %s", got, want)
	}
	// lossless base64 always present
	if !contains(got, `"data_b64":"eyJuYW1lIjoiQWRhIn0="`) {
		t.Errorf("missing data_b64\n got: %s", got)
	}
	if !contains(got, `"timestamp":"2026-05-20T14:03:11.123Z"`) {
		t.Errorf("bad timestamp\n got: %s", got)
	}
}

func TestEventBase64Only(t *testing.T) {
	e := streams.RecordedEvent{
		Data:            []byte(`{"name":"Ada"}`),
		DataContentType: "application/json",
	}
	got := mustJSON(t, Event(e, Base64))
	if contains(got, `"data":`) {
		t.Errorf("base64 mode must not emit decoded data\n got: %s", got)
	}
	if !contains(got, `"data_b64":`) {
		t.Errorf("missing data_b64\n got: %s", got)
	}
}

func TestEventNonUTF8FallsBackToB64Only(t *testing.T) {
	e := streams.RecordedEvent{
		Data:            []byte{0xff, 0xfe, 0x00},
		DataContentType: "application/octet-stream",
	}
	got := mustJSON(t, Event(e, Auto))
	if contains(got, `"data":`) {
		t.Errorf("non-utf8 data must not be decoded\n got: %s", got)
	}
}

func TestEventZeroTimeIsNullAndHashOmitted(t *testing.T) {
	got := mustJSON(t, Event(streams.RecordedEvent{}, Auto))
	if !contains(got, `"epoch":null`) {
		t.Errorf("zero time should be null\n got: %s", got)
	}
	if contains(got, `prev_event_hash`) {
		t.Errorf("empty hash must be omitted\n got: %s", got)
	}
	if !contains(got, `"tags":[]`) {
		t.Errorf("nil tags should render []\n got: %s", got)
	}
}

func TestInstance(t *testing.T) {
	i := stores.Instance{
		StoreID: "default_store", Node: "reckon@beam01", Mode: stores.ModeCluster,
		DataDir: "/bulk0/reckon", Timeout: 5 * time.Second, RegisteredAt: ts,
	}
	got := mustJSON(t, Instance(i))
	for _, want := range []string{
		`"store_id":"default_store"`, `"mode":"cluster"`, `"timeout_ms":5000`,
		`"registered_at":"2026-05-20T14:03:11.123Z"`,
	} {
		if !contains(got, want) {
			t.Errorf("instance missing %s\n got: %s", want, got)
		}
	}
}

func TestParseBytes(t *testing.T) {
	for _, c := range []struct {
		in   string
		mode Bytes
		ok   bool
	}{{"auto", Auto, true}, {"", Auto, true}, {"base64", Base64, true}, {"nope", Auto, false}} {
		m, ok := ParseBytes(c.in)
		if ok != c.ok || (ok && m != c.mode) {
			t.Errorf("ParseBytes(%q) = %v,%v want %v,%v", c.in, m, ok, c.mode, c.ok)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
