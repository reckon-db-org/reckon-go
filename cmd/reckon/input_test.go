package main

import (
	"strings"
	"testing"

	"codeberg.org/reckon-db-org/reckon-go/streams"
	"codeberg.org/reckon-db-org/reckon-go/subscriptions"
)

func TestParseExpected(t *testing.T) {
	cases := map[string]streams.ExpectedVersion{
		"no-stream": streams.NoStream,
		"any":       streams.AnyVersion,
		"":          streams.AnyVersion,
		"exists":    streams.StreamExists,
		"7":         streams.ExpectedVersion(7),
	}
	for in, want := range cases {
		got, err := parseExpected(in)
		if err != nil || got != want {
			t.Errorf("parseExpected(%q) = %v,%v want %v", in, got, err, want)
		}
	}
	if _, err := parseExpected("nope"); err == nil {
		t.Error("parseExpected(nope) should error")
	}
}

func TestParseSubType(t *testing.T) {
	if ty, err := parseSubType("event_type"); err != nil || ty != subscriptions.TypeEventType {
		t.Errorf("event_type -> %v,%v", ty, err)
	}
	if _, err := parseSubType("bogus"); err == nil {
		t.Error("bogus sub type should error")
	}
}

func TestParseTime(t *testing.T) {
	rfc, err := parseTime("2026-05-20T14:03:11Z")
	if err != nil || rfc.UTC().Hour() != 14 {
		t.Errorf("rfc3339 parse: %v %v", rfc, err)
	}
	ms, err := parseTime("1747749600000")
	if err != nil || ms.UnixMilli() != 1747749600000 {
		t.Errorf("epoch-ms parse: %v %v", ms, err)
	}
	if _, err := parseTime("not-a-time"); err == nil {
		t.Error("garbage timestamp should error")
	}
}

func TestKVFlag(t *testing.T) {
	k := kvFlag{}
	if err := k.Set("a=1"); err != nil {
		t.Fatal(err)
	}
	if err := k.Set("b=x=y"); err != nil { // value may contain '='
		t.Fatal(err)
	}
	if k["a"] != "1" || k["b"] != "x=y" {
		t.Errorf("kvFlag wrong: %v", k)
	}
	if err := k.Set("noequals"); err == nil {
		t.Error("missing '=' should error")
	}
}

func TestReadProposedEventsArray(t *testing.T) {
	in := `[{"event_type":"user_registered_v1","data":{"name":"Ada"},"tags":["u"]},
	        {"event_type":"user_promoted_v1","data_b64":"aGVsbG8="}]`
	evs, err := readProposedEvents(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 {
		t.Fatalf("want 2 events, got %d", len(evs))
	}
	if string(evs[0].Data) != `{"name":"Ada"}` {
		t.Errorf("event 0 data = %q", evs[0].Data)
	}
	if evs[0].DataContentType != "application/json" {
		t.Errorf("default content type missing: %q", evs[0].DataContentType)
	}
	if evs[0].EventID == "" {
		t.Error("missing event_id should be auto-generated")
	}
	if string(evs[1].Data) != "hello" { // base64 "aGVsbG8=" decodes to hello
		t.Errorf("event 1 data = %q", evs[1].Data)
	}
}

func TestReadProposedEventsNDJSON(t *testing.T) {
	in := `{"event_type":"a","data":{"x":1}}
{"event_type":"b","data":"plain"}`
	evs, err := readProposedEvents(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 || evs[0].EventType != "a" || string(evs[1].Data) != "plain" {
		t.Errorf("NDJSON parse wrong: %+v", evs)
	}
}

func TestGenUUIDFormat(t *testing.T) {
	id := genUUID()
	parts := strings.Split(id, "-")
	if len(parts) != 5 || len(id) != 36 {
		t.Errorf("bad uuid %q", id)
	}
	if parts[2][0] != '4' { // version nibble
		t.Errorf("expected v4 uuid, got %q", id)
	}
	if genUUID() == id {
		t.Error("uuids should differ")
	}
}
