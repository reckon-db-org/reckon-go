package streams

import "context"

// Reserved metadata keys for the Enterprise Integration Patterns
// correlation/causation identifiers. These are the canonical JSON keys
// documented authoritatively in reckon-proto (reckon_shared.proto,
// "Reserved metadata keys"); use these constants rather than typing the
// strings, so every client agrees on the names.
//
// They are plain metadata keys, not a special store feature. A raw client
// with no CQRS framework sets them itself when appending (see WithLineage);
// a BEAM/evoq application has them set and propagated automatically. The
// store never interprets them.
const (
	// CausationIDKey: event_id of the message that DIRECTLY caused this
	// event (one hop).
	CausationIDKey = "causation_id"
	// CorrelationIDKey: shared id grouping every event in one conversation,
	// copied forward unchanged.
	CorrelationIDKey = "correlation_id"
	// ConversationIDKey: optional higher-level, usually domain-specific
	// grouping that ties multiple correlations to one conceptual operation.
	ConversationIDKey = "conversation_id"
)

// ReadEffects returns the events DIRECTLY caused by messageID (those whose
// metadata causation_id equals it). One hop; compose repeatedly to walk a
// chain. Convenience over [Client.ReadByMetadata] with [CausationIDKey].
func (c *Client) ReadEffects(ctx context.Context, messageID string, batch uint64) ([]RecordedEvent, error) {
	return c.ReadByMetadata(ctx, CausationIDKey, messageID, batch)
}

// ReadCorrelated returns every event in the conversation correlationID
// (those whose metadata correlation_id equals it). Convenience over
// [Client.ReadByMetadata] with [CorrelationIDKey].
func (c *Client) ReadCorrelated(ctx context.Context, correlationID string, batch uint64) ([]RecordedEvent, error) {
	return c.ReadByMetadata(ctx, CorrelationIDKey, correlationID, batch)
}

// ReadConversation returns every event tied to the conversation
// conversationID (those whose metadata conversation_id equals it).
// Requires the producer to set, and the store to index, conversation_id.
func (c *Client) ReadConversation(ctx context.Context, conversationID string, batch uint64) ([]RecordedEvent, error) {
	return c.ReadByMetadata(ctx, ConversationIDKey, conversationID, batch)
}

// WithLineage returns a copy of meta with the lineage keys set (skipping
// empty values), for building a [ProposedEvent.Metadata] map before
// marshalling it to JSON. A convenience so callers don't type the key
// strings; it does NOT auto-propagate (a raw client has no notion of "the
// message I am handling" — that is a framework's job). Pass an empty string
// to leave a key unset.
//
//	m := streams.WithLineage(map[string]any{"tenant": "acme"}, causingID, corrID)
//	b, _ := json.Marshal(m)
//	ev := streams.ProposedEvent{EventType: "...", Data: data, Metadata: b}
func WithLineage(meta map[string]any, causationID, correlationID string) map[string]any {
	if meta == nil {
		meta = map[string]any{}
	}
	if causationID != "" {
		meta[CausationIDKey] = causationID
	}
	if correlationID != "" {
		meta[CorrelationIDKey] = correlationID
	}
	return meta
}
