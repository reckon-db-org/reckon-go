//go:build e2e

// End-to-end tests against a live gateway (the running fleet). Gated behind
// the `e2e` build tag so default `go test` stays hermetic.
//
//	RECKON_ENDPOINT=beam01.lab:50051 RECKON_STORE=default_store \
//	  go test -tags e2e ./cmd/reckon/...
//
// They exercise the real binary path via run() — full routing + encode +
// JSON shaping — and assert the contract from plans/DESIGN_RECKON_CLI.md.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func endpoint() string { return env("RECKON_ENDPOINT", "beam01.lab:50051") }
func store() string    { return env("RECKON_STORE", "default_store") }

// invoke runs the CLI in-process with the fleet endpoint/store injected as
// global flags, returning stdout, stderr and the exit code.
func invoke(t *testing.T, args ...string) (string, string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	full := append([]string{"-e", endpoint(), "-s", store()}, args...)
	var out, errOut bytes.Buffer
	code := run(ctx, full, nil, &out, &errOut)
	return out.String(), errOut.String(), code
}

func TestE2EStoresList(t *testing.T) {
	out, errOut, code := invoke(t, "stores", "list")
	if code != 0 {
		t.Fatalf("stores list exit %d, stderr=%s", code, errOut)
	}
	var insts []map[string]any
	if err := json.Unmarshal([]byte(out), &insts); err != nil {
		t.Fatalf("stores list output is not a JSON array: %v\n%s", err, out)
	}
	if len(insts) == 0 {
		t.Skip("no stores announced on fleet; nothing to assert")
	}
	for _, want := range []string{"store_id", "node", "mode", "registered_at"} {
		if _, ok := insts[0][want]; !ok {
			t.Errorf("instance missing %q: %v", want, insts[0])
		}
	}
}

func TestE2EStreamsReadShape(t *testing.T) {
	// Read against the configured store; either we get a JSON array of events
	// or a typed not_found — both are valid contract outcomes.
	out, errOut, code := invoke(t, "streams", "read", "$nonexistent-"+time.Now().Format("150405"), "--count", "1")
	switch code {
	case 0:
		var evs []map[string]any
		if err := json.Unmarshal([]byte(out), &evs); err != nil {
			t.Fatalf("streams read output not a JSON array: %v\n%s", err, out)
		}
	case 3: // not_found is acceptable for a missing stream
		var e map[string]any
		if json.Unmarshal([]byte(errOut), &e) != nil {
			t.Fatalf("not_found error not valid JSON: %s", errOut)
		}
	default:
		t.Fatalf("unexpected exit %d, stderr=%s", code, errOut)
	}
}

// decodeArray asserts stdout is a JSON array and returns it.
func decodeArray(t *testing.T, out string) []map[string]any {
	t.Helper()
	var arr []map[string]any
	if err := json.Unmarshal([]byte(out), &arr); err != nil {
		t.Fatalf("output not a JSON array: %v\n%s", err, out)
	}
	return arr
}

func TestE2EStreamsList(t *testing.T) {
	out, errOut, code := invoke(t, "streams", "list")
	if code != 0 {
		t.Fatalf("streams list exit %d, stderr=%s", code, errOut)
	}
	var ids []string
	if err := json.Unmarshal([]byte(out), &ids); err != nil {
		t.Fatalf("streams list not a JSON array of strings: %v\n%s", err, out)
	}
}

func TestE2EAdminStats(t *testing.T) {
	out, errOut, code := invoke(t, "admin", "stats")
	if code != 0 {
		t.Fatalf("admin stats exit %d, stderr=%s", code, errOut)
	}
	var s map[string]any
	if err := json.Unmarshal([]byte(out), &s); err != nil {
		t.Fatalf("admin stats not an object: %v\n%s", err, out)
	}
	for _, k := range []string{"total_streams", "total_events", "total_subscriptions", "total_snapshots"} {
		if _, ok := s[k]; !ok {
			t.Errorf("admin stats missing %q: %v", k, s)
		}
	}
}

func TestE2ESubsList(t *testing.T) {
	out, errOut, code := invoke(t, "subs", "list")
	if code != 0 {
		t.Fatalf("subs list exit %d, stderr=%s", code, errOut)
	}
	decodeArray(t, out)
}

func TestE2ESchemaList(t *testing.T) {
	out, errOut, code := invoke(t, "schema", "list")
	if code != 0 {
		t.Fatalf("schema list exit %d, stderr=%s", code, errOut)
	}
	decodeArray(t, out)
}

func TestE2EHealthServerInfo(t *testing.T) {
	out, errOut, code := invoke(t, "health", "server-info")
	if code != 0 {
		t.Fatalf("health server-info exit %d, stderr=%s", code, errOut)
	}
	var s map[string]any
	if err := json.Unmarshal([]byte(out), &s); err != nil {
		t.Fatalf("server-info not an object: %v\n%s", err, out)
	}
	if _, ok := s["reckon_gateway_version"]; !ok {
		t.Errorf("server-info missing reckon_gateway_version: %v", s)
	}
}

func TestE2ECatalogueStatus(t *testing.T) {
	// gateway-wide: no --store needed
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var out, errOut bytes.Buffer
	code := run(ctx, []string{"-e", endpoint(), "catalogue", "status"}, nil, &out, &errOut)
	if code != 0 {
		t.Fatalf("catalogue status exit %d, stderr=%s", code, errOut.String())
	}
	var s map[string]any
	if err := json.Unmarshal(out.Bytes(), &s); err != nil {
		t.Fatalf("catalogue status not an object: %v\n%s", err, out.String())
	}
	if _, ok := s["clusters"]; !ok {
		t.Errorf("catalogue status missing clusters: %v", s)
	}
}

func TestE2EStreamsWatchReadyFrame(t *testing.T) {
	// Watch is unbounded; cancel after a short window and assert we at least
	// saw the leading "ready" frame and a clean terminal frame.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	full := []string{"-e", endpoint(), "-s", store(), "streams", "watch",
		"$probe-" + time.Now().Format("150405"), "--from", "0"}
	var out, errOut bytes.Buffer
	_ = run(ctx, full, nil, &out, &errOut)

	fr := frames(t, out.String())
	if len(fr) == 0 {
		t.Fatalf("no frames emitted; stderr=%s", errOut.String())
	}
	if fr[0]["type"] != "ready" {
		t.Errorf("first frame must be ready, got %v", fr[0])
	}
	last := fr[len(fr)-1]["type"]
	if last != "end" && last != "error" {
		t.Errorf("stream must close with end|error, got %v", last)
	}
}
