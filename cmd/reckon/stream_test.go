package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/reckon-db-org/reckon-go/cmd/reckon/encode"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// frames decodes NDJSON output into a slice of frame maps.
func frames(t *testing.T, s string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("bad NDJSON line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestPumpCleanEOF(t *testing.T) {
	events := make(chan int, 3)
	errs := make(chan error, 1)
	for _, v := range []int{1, 2, 3} {
		events <- v
	}
	close(events)
	errs <- nil

	var buf bytes.Buffer
	err := pump(context.Background(), &buf, events, errs, func(v int) map[string]any {
		return map[string]any{"type": "event", "n": v}
	})
	if err != nil {
		t.Fatalf("pump returned error: %v", err)
	}
	fr := frames(t, buf.String())
	if len(fr) != 5 {
		t.Fatalf("want ready+3+end = 5 frames, got %d: %s", len(fr), buf.String())
	}
	if fr[0]["type"] != "ready" {
		t.Errorf("first frame must be ready, got %v", fr[0]["type"])
	}
	if last := fr[4]; last["type"] != "end" || last["reason"] != "eof" {
		t.Errorf("last frame must be end/eof, got %v", last)
	}
}

func TestPumpErrorFrame(t *testing.T) {
	events := make(chan int)
	errs := make(chan error, 1)
	close(events)
	errs <- status.Error(codes.Unavailable, "cluster down")

	var buf bytes.Buffer
	err := pump(context.Background(), &buf, events, errs, func(v int) map[string]any {
		return map[string]any{"type": "event", "n": v}
	})
	if err == nil {
		t.Fatal("expected error from pump")
	}
	ce, ok := err.(*cliError)
	if !ok || ce.Exit != 4 || ce.Code != "unavailable" {
		t.Fatalf("want unavailable/exit4, got %+v", err)
	}
	fr := frames(t, buf.String())
	last := fr[len(fr)-1]
	if last["type"] != "error" || last["code"] != "unavailable" {
		t.Errorf("last frame must be error/unavailable, got %v", last)
	}
}

func TestPumpCanceledIsCleanStop(t *testing.T) {
	events := make(chan int)
	errs := make(chan error, 1)
	close(events)
	errs <- status.Error(codes.Canceled, "ctx canceled")

	var buf bytes.Buffer
	err := pump(context.Background(), &buf, events, errs, func(v int) map[string]any {
		return map[string]any{"type": "event"}
	})
	if err != nil {
		t.Fatalf("cancellation must be a clean stop, got %v", err)
	}
	fr := frames(t, buf.String())
	last := fr[len(fr)-1]
	if last["type"] != "end" || last["reason"] != "client_eof" {
		t.Errorf("want end/client_eof, got %v", last)
	}
}

func TestFromGRPCMapping(t *testing.T) {
	cases := []struct {
		code codes.Code
		name string
		exit int
	}{
		{codes.NotFound, "not_found", 3},
		{codes.Unavailable, "unavailable", 4},
		{codes.DeadlineExceeded, "timeout", 5},
		{codes.FailedPrecondition, "precondition", 8},
		{codes.Internal, "internal", 1},
	}
	for _, c := range cases {
		ce := fromGRPC(status.Error(c.code, "x"))
		if ce.Code != c.name || ce.Exit != c.exit {
			t.Errorf("%v -> %s/%d, want %s/%d", c.code, ce.Code, ce.Exit, c.name, c.exit)
		}
	}
	if !isCanceled(status.Error(codes.Canceled, "x")) {
		t.Error("isCanceled should match codes.Canceled")
	}
	if !isCanceled(context.Canceled) {
		t.Error("isCanceled should match context.Canceled")
	}
	if isCanceled(errors.New("nope")) {
		t.Error("isCanceled false positive")
	}
}

func TestRunUsageErrors(t *testing.T) {
	// Hermetic: ignore any RECKON_* in the ambient env (e.g. e2e runs).
	t.Setenv("RECKON_STORE", "")
	t.Setenv("RECKON_ENDPOINT", "")
	var out, errOut bytes.Buffer
	// missing group+command
	if code := run(context.Background(), nil, nil, &out, &errOut); code != 2 {
		t.Errorf("no args: want exit 2, got %d", code)
	}
	// unknown command
	errOut.Reset()
	if code := run(context.Background(), []string{"streams", "frobnicate"}, nil, &out, &errOut); code != 2 {
		t.Errorf("unknown cmd: want exit 2, got %d", code)
	}
	fr := frames(t, errOut.String())
	if e, _ := fr[0]["error"].(map[string]any); e == nil || e["code"] != "usage" {
		t.Errorf("want usage error object, got %s", errOut.String())
	}
	// store-bound command without --store
	errOut.Reset()
	if code := run(context.Background(), []string{"streams", "read", "s1"}, nil, &out, &errOut); code != 2 {
		t.Errorf("missing store: want exit 2, got %d", code)
	}
}

func TestVersionFlag(t *testing.T) {
	for _, flag := range []string{"--version", "-V"} {
		var out, errOut bytes.Buffer
		if code := run(context.Background(), []string{flag}, nil, &out, &errOut); code != 0 {
			t.Fatalf("%s: exit %d, stderr=%s", flag, code, errOut.String())
		}
		var m map[string]any
		if err := json.Unmarshal(out.Bytes(), &m); err != nil {
			t.Fatalf("%s: output not JSON: %v\n%s", flag, err, out.String())
		}
		if _, ok := m["client"]; !ok {
			t.Errorf("%s: missing client field: %v", flag, m)
		}
	}
}

func TestParseGlobalFlagsAndAliases(t *testing.T) {
	o, rest, err := parseGlobal([]string{"-e", "h:1", "-s", "st", "--bytes", "base64", "streams", "read", "x"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.endpoint != "h:1" || o.store != "st" {
		t.Errorf("aliases not applied: %+v", o)
	}
	if o.bytes != encode.Base64 {
		t.Errorf("--bytes base64 not applied: %v", o.bytes)
	}
	if len(rest) != 3 || rest[0] != "streams" || rest[2] != "x" {
		t.Errorf("rest wrong: %v", rest)
	}
}

func TestTransportDefaultsToTLS(t *testing.T) {
	o, _, err := parseGlobal([]string{"streams", "read", "x"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.plaintext {
		t.Fatal("plaintext must not be the default")
	}
	dialOpts, err := o.transport()
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	if dialOpts != nil {
		t.Errorf("default transport should defer to Connect's TLS default, got %d opts", len(dialOpts))
	}
}

func TestTransportPlaintextExplicit(t *testing.T) {
	o, _, err := parseGlobal([]string{"--plaintext", "streams", "read", "x"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !o.plaintext {
		t.Fatal("--plaintext not applied")
	}
	dialOpts, err := o.transport()
	if err != nil || len(dialOpts) != 1 {
		t.Fatalf("expected single insecure dial opt, got %v / %v", dialOpts, err)
	}
}

func TestPlaintextConflictsWithTLSFlags(t *testing.T) {
	if _, _, err := parseGlobal([]string{"--plaintext", "--ca", "x.pem", "streams", "read", "x"}); err == nil {
		t.Fatal("expected --plaintext/--ca conflict error")
	}
	if _, _, err := parseGlobal([]string{"--plaintext", "--server-name", "gw", "streams", "read", "x"}); err == nil {
		t.Fatal("expected --plaintext/--server-name conflict error")
	}
}
