// Package encode renders reckon-go domain types into the JSON shapes
// defined by plans/DESIGN_RECKON_CLI.md. It is the single source of truth
// for field names, timestamp formatting, and the byte-field policy (§4.3),
// so the wire shape can be golden-tested without touching the network.
package encode

import (
	"encoding/base64"
	"encoding/json"
	"time"
	"unicode/utf8"

	"codeberg.org/reckon-db-org/reckon-go/stores"
	"codeberg.org/reckon-db-org/reckon-go/streams"
)

// Bytes selects how byte-valued fields are rendered.
type Bytes int

const (
	// Auto emits "<field>_b64" (lossless) plus a decoded "<field>" when the
	// content-type is application/json (parsed) or the bytes are valid UTF-8
	// (string). Default; what reckon-nvim consumes.
	Auto Bytes = iota
	// Base64 emits only "<field>_b64". Lossless and stable for round-tripping.
	Base64
)

// ParseBytes maps a --bytes flag value to a Bytes mode.
func ParseBytes(s string) (Bytes, bool) {
	switch s {
	case "auto", "":
		return Auto, true
	case "base64":
		return Base64, true
	default:
		return Auto, false
	}
}

// Time formats t as an RFC3339 nanosecond UTC string, or nil when zero.
func Time(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// Event renders a recorded event (DESIGN §4.2).
func Event(e streams.RecordedEvent, mode Bytes) map[string]any {
	m := map[string]any{
		"event_id":              e.EventID,
		"event_type":            e.EventType,
		"stream_id":             e.StreamID,
		"version":               e.Version,
		"tags":                  tags(e.Tags),
		"timestamp":             Time(e.Timestamp),
		"epoch":                 Time(e.Epoch),
		"data_content_type":     e.DataContentType,
		"metadata_content_type": e.MetadataContentType,
	}
	addBytes(m, "data", e.Data, e.DataContentType, mode)
	addBytes(m, "metadata", e.Metadata, e.MetadataContentType, mode)
	addHash(m, "prev_event_hash", e.PrevEventHash)
	return m
}

// Instance renders a store instance (DESIGN §4.4).
func Instance(i stores.Instance) map[string]any {
	return map[string]any{
		"store_id":      i.StoreID,
		"node":          i.Node,
		"mode":          string(i.Mode),
		"data_dir":      i.DataDir,
		"timeout_ms":    i.Timeout.Milliseconds(),
		"registered_at": Time(i.RegisteredAt),
	}
}

// StoreEvent renders a topology change as a NDJSON "store_event" frame body.
func StoreEvent(e stores.Event) map[string]any {
	return map[string]any{
		"type":     "store_event",
		"change":   string(e.Type),
		"instance": Instance(e.Instance),
		"at":       Time(e.At),
	}
}

func tags(t []string) []string {
	if t == nil {
		return []string{}
	}
	return t
}

// addBytes implements the §4.3 policy for data/metadata fields. The "_b64"
// key is always present (lossless recovery); in Auto mode a decoded
// convenience value is added when the bytes are JSON or valid UTF-8.
func addBytes(m map[string]any, key string, b []byte, contentType string, mode Bytes) {
	m[key+"_b64"] = base64.StdEncoding.EncodeToString(b)
	if mode != Auto || len(b) == 0 {
		return
	}
	if contentType == "application/json" && json.Valid(b) {
		m[key] = json.RawMessage(b)
		return
	}
	if utf8.Valid(b) {
		m[key] = string(b)
	}
}

// addBlob is addBytes for fields without a content-type (schema, snapshot
// data/metadata): always "<key>_b64", plus a decoded value in Auto mode when
// the bytes parse as JSON, else when they are valid UTF-8.
func addBlob(m map[string]any, key string, b []byte, mode Bytes) {
	m[key+"_b64"] = base64.StdEncoding.EncodeToString(b)
	if mode != Auto || len(b) == 0 {
		return
	}
	if json.Valid(b) {
		m[key] = json.RawMessage(b)
		return
	}
	if utf8.Valid(b) {
		m[key] = string(b)
	}
}

// addHash emits only "<key>_b64"; omitted entirely when empty (§4.2).
func addHash(m map[string]any, key string, b []byte) {
	if len(b) == 0 {
		return
	}
	m[key+"_b64"] = base64.StdEncoding.EncodeToString(b)
}
