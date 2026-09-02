package main

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/reckon-db-org/reckon-go/streams"
	"github.com/reckon-db-org/reckon-go/subscriptions"
)

// parseExpected maps the --expect flag to an ExpectedVersion (DESIGN §3).
func parseExpected(s string) (streams.ExpectedVersion, error) {
	switch s {
	case "no-stream":
		return streams.NoStream, nil
	case "any", "":
		return streams.AnyVersion, nil
	case "exists":
		return streams.StreamExists, nil
	default:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid --expect %q (want no-stream|any|exists|<N>)", s)
		}
		return streams.ExpectedVersion(n), nil
	}
}

// parseU64 parses a positional unsigned integer argument.
func parseU64(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

// parseSubType maps --type to a subscription Type.
func parseSubType(s string) (subscriptions.Type, error) {
	switch subscriptions.Type(s) {
	case subscriptions.TypeStream, subscriptions.TypeEventType, subscriptions.TypeEventPattern,
		subscriptions.TypeEventPayload, subscriptions.TypeTags:
		return subscriptions.Type(s), nil
	default:
		return "", fmt.Errorf("invalid --type %q (want stream|event_type|event_pattern|event_payload|tags)", s)
	}
}

// parseTime accepts RFC3339(nano) or epoch milliseconds (DESIGN §3).
func parseTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(ms), nil
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q (want RFC3339 or epoch-ms)", s)
}

// kvFlag collects repeated --opt key=value pairs.
type kvFlag map[string]string

func (k kvFlag) String() string { return "" }
func (k kvFlag) Set(s string) error {
	key, val, ok := strings.Cut(s, "=")
	if !ok {
		return fmt.Errorf("expected key=value, got %q", s)
	}
	k[key] = val
	return nil
}

// proposedInput is the stdin JSON shape for events (DESIGN §4.7).
type proposedInput struct {
	EventID             string          `json:"event_id"`
	EventType           string          `json:"event_type"`
	Data                json.RawMessage `json:"data"`
	DataB64             string          `json:"data_b64"`
	Metadata            json.RawMessage `json:"metadata"`
	MetadataB64         string          `json:"metadata_b64"`
	Tags                []string        `json:"tags"`
	DataContentType     string          `json:"data_content_type"`
	MetadataContentType string          `json:"metadata_content_type"`
}

// readProposedEvents parses stdin as a JSON array OR NDJSON of proposed events.
func readProposedEvents(r io.Reader) ([]streams.ProposedEvent, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return nil, fmt.Errorf("no input")
	}

	var inputs []proposedInput
	if strings.HasPrefix(trimmed, "[") {
		if err := json.Unmarshal([]byte(trimmed), &inputs); err != nil {
			return nil, err
		}
	} else {
		sc := bufio.NewScanner(strings.NewReader(trimmed))
		sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var pi proposedInput
			if err := json.Unmarshal([]byte(line), &pi); err != nil {
				return nil, err
			}
			inputs = append(inputs, pi)
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}

	out := make([]streams.ProposedEvent, 0, len(inputs))
	for i, pi := range inputs {
		data, err := resolveInputBytes(pi.Data, pi.DataB64)
		if err != nil {
			return nil, fmt.Errorf("event %d data: %w", i, err)
		}
		meta, err := resolveInputBytes(pi.Metadata, pi.MetadataB64)
		if err != nil {
			return nil, fmt.Errorf("event %d metadata: %w", i, err)
		}
		id := pi.EventID
		if id == "" {
			id = genUUID()
		}
		out = append(out, streams.ProposedEvent{
			EventID:             id,
			EventType:           pi.EventType,
			Data:                data,
			Metadata:            meta,
			Tags:                pi.Tags,
			DataContentType:     defaultStr(pi.DataContentType, "application/json"),
			MetadataContentType: defaultStr(pi.MetadataContentType, "application/json"),
		})
	}
	return out, nil
}

// resolveInputBytes prefers the base64 form; otherwise a JSON string yields its
// decoded text and any other JSON value yields its raw bytes.
func resolveInputBytes(raw json.RawMessage, b64 string) ([]byte, error) {
	if b64 != "" {
		return base64.StdEncoding.DecodeString(b64)
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return []byte(s), nil
	}
	return []byte(raw), nil
}

func defaultStr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// proposedToRecorded maps proposed events to recorded events for schema
// upcasting, which takes recorded events. Only the fields the upcaster needs
// (type, data, metadata, content types, tags) are carried.
func proposedToRecorded(ps []streams.ProposedEvent) []streams.RecordedEvent {
	out := make([]streams.RecordedEvent, len(ps))
	for i, p := range ps {
		out[i] = streams.RecordedEvent{
			EventID:             p.EventID,
			EventType:           p.EventType,
			Data:                p.Data,
			Metadata:            p.Metadata,
			Tags:                p.Tags,
			DataContentType:     p.DataContentType,
			MetadataContentType: p.MetadataContentType,
		}
	}
	return out
}

// genUUID returns a random RFC-4122 v4 UUID (stdlib-only, no dep).
func genUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
