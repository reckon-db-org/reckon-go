// Package dcb wraps the gRPC DcbService with idiomatic Go types.
//
// DCB (Dynamic Consistency Boundary) is ReckonDB's complement to
// per-aggregate optimistic concurrency. Where a Streams.Append checks
// "no new events on THIS stream since version N", a
// DCB.AppendIfNoTagMatches checks "no event matching THIS tag-filter
// has been written since seq N". Use it for cross-cutting invariants
// (uniqueness, allocation, rate-limit, eligibility) where the
// consistency boundary doesn't live inside a single aggregate.
//
// The canonical Decision loop:
//
//	d := c.Dcb("orders")
//	for retries := 0; retries < budget; retries++ {
//	    ctx, _ := d.Read(rootCtx, dcb.MatchAny("slot:42"), 100)
//	    // ...domain logic decides whether to commit, with what events...
//	    committed, conflict, err := d.Append(
//	        rootCtx,
//	        dcb.MatchAny("slot:42"),
//	        ctx.MaxSeq,
//	        []dcb.ProposedEvent{...},
//	    )
//	    switch {
//	    case err != nil:
//	        return err              // transport / backend failure
//	    case conflict != nil:
//	        continue                // context stale, re-read and retry
//	    default:
//	        return committed        // success
//	    }
//	}
//
// Conflict is NOT an error: it's a structured response carrying the
// new MaxSeq the caller should use to refresh its cutoff. err covers
// only transport-level failures (UNAVAILABLE, UNIMPLEMENTED, INTERNAL,
// etc.).
package dcb

import (
	"context"
	"errors"

	pb "codeberg.org/reckon-db-org/reckon-go/genproto/gatewayv1"
	"codeberg.org/reckon-db-org/reckon-go/streams"
	"google.golang.org/grpc"
)

// SeqCutoffSawNothing is the sentinel for "I have observed no events
// matching this filter yet." Pass it as the cutoff to Append when
// you're trying to write an event that must be the FIRST match.
const SeqCutoffSawNothing int64 = -1

// TagFilter is a recursive predicate over an event's tag set or type.
//
// Construct via [MatchAny], [MatchAll], [EventType], [And], [Or].
// Filters compose to arbitrary depth.
type TagFilter interface {
	toProto() *pb.TagFilter
	isTagFilter()
}

type matchAny struct{ tags []string }
type matchAll struct{ tags []string }
type eventTypeMatch struct{ eventType string }
type conjunction struct{ subs []TagFilter }
type disjunction struct{ subs []TagFilter }

func (matchAny) isTagFilter()       {}
func (matchAll) isTagFilter()       {}
func (eventTypeMatch) isTagFilter() {}
func (conjunction) isTagFilter()    {}
func (disjunction) isTagFilter()    {}

// MatchAny matches an event whose tag set contains ANY of tags.
// Equivalent to the Erlang term {any_of, [Tag]}.
func MatchAny(tags ...string) TagFilter { return matchAny{tags: tags} }

// MatchAll matches an event whose tag set contains ALL of tags.
// Equivalent to the Erlang term {all_of, [Tag]}.
func MatchAll(tags ...string) TagFilter { return matchAll{tags: tags} }

// EventType matches an event whose event_type equals t.
// Equivalent to the Erlang term {event_type, binary()}.
// Requires backing reckon-db 5.2.0+ ([by_event_type] index).
func EventType(t string) TagFilter { return eventTypeMatch{eventType: t} }

// And matches an event satisfying ALL of filters. Equivalent to the
// Erlang term {and_, [TagFilter]}.
func And(filters ...TagFilter) TagFilter { return conjunction{subs: filters} }

// Or matches an event satisfying ANY of filters. Equivalent to the
// Erlang term {or_, [TagFilter]}.
func Or(filters ...TagFilter) TagFilter { return disjunction{subs: filters} }

func (f matchAny) toProto() *pb.TagFilter {
	return &pb.TagFilter{Kind: &pb.TagFilter_MatchAny{
		MatchAny: &pb.TagList{Tags: f.tags},
	}}
}
func (f matchAll) toProto() *pb.TagFilter {
	return &pb.TagFilter{Kind: &pb.TagFilter_MatchAll{
		MatchAll: &pb.TagList{Tags: f.tags},
	}}
}
func (f eventTypeMatch) toProto() *pb.TagFilter {
	return &pb.TagFilter{Kind: &pb.TagFilter_EventTypeMatch{
		EventTypeMatch: f.eventType,
	}}
}
func (f conjunction) toProto() *pb.TagFilter {
	return &pb.TagFilter{Kind: &pb.TagFilter_Conjunction{
		Conjunction: subsToFilterList(f.subs),
	}}
}
func (f disjunction) toProto() *pb.TagFilter {
	return &pb.TagFilter{Kind: &pb.TagFilter_Disjunction{
		Disjunction: subsToFilterList(f.subs),
	}}
}

func subsToFilterList(subs []TagFilter) *pb.FilterList {
	out := make([]*pb.TagFilter, 0, len(subs))
	for _, s := range subs {
		out = append(out, s.toProto())
	}
	return &pb.FilterList{Filters: out}
}

// ProposedEvent is an event to be appended under the DCB pseudo-stream.
// Zero-value fields mean "let the server decide" (server generates
// EventID if empty; content-type defaults to "application/json").
type ProposedEvent struct {
	EventID             string
	EventType           string
	Data                []byte
	Metadata            []byte
	Tags                []string
	DataContentType     string
	MetadataContentType string
}

func (e ProposedEvent) toProto() *pb.ProposedEvent {
	return &pb.ProposedEvent{
		EventId:             e.EventID,
		EventType:           e.EventType,
		Data:                e.Data,
		Metadata:            e.Metadata,
		Tags:                e.Tags,
		DataContentType:     e.DataContentType,
		MetadataContentType: e.MetadataContentType,
	}
}

// Committed is the success outcome of [Client.Append]: the
// conditional-append wrote everything atomically and LastSeq is the
// seq assigned to the final event. Use LastSeq as the cutoff for a
// follow-up Append against an overlapping tag set without re-reading.
type Committed struct {
	LastSeq uint64
}

// Conflict is the "your context is stale" outcome of [Client.Append].
// The server observed an event matching the filter with seq strictly
// above the supplied cutoff; nothing was written. MaxSeq is the
// highest matching seq the server saw — use it (or a higher value
// from a fresh [Client.Read]) as the cutoff on retry.
type Conflict struct {
	MaxSeq uint64
}

// ReadResult is what [Client.Read] returns: events matching the
// filter from the DCB pseudo-stream (ordered by seq ascending) plus
// the highest seq observed across them. MaxSeq is [SeqCutoffSawNothing]
// when Events is empty, ready to pass to a follow-up [Client.Append].
type ReadResult struct {
	Events []streams.RecordedEvent
	MaxSeq int64
}

// Client is the typed wrapper for one store. Construct via [New].
type Client struct {
	raw   pb.DcbServiceClient
	store string
}

// New returns a Client that issues RPCs over conn, scoped to storeID.
func New(conn grpc.ClientConnInterface, storeID string) *Client {
	return &Client{raw: pb.NewDcbServiceClient(conn), store: storeID}
}

// StoreID returns the store this Client is bound to.
func (c *Client) StoreID() string { return c.store }

// Append conditionally writes events under the DCB pseudo-stream.
//
// The write succeeds (returns a non-nil *Committed) iff no event
// matching filter has seq strictly above cutoff. Otherwise returns a
// non-nil *Conflict carrying the highest matching seq observed; the
// events are NOT written. err covers transport / backend failures
// only (UNAVAILABLE, UNIMPLEMENTED, INTERNAL, NOT_FOUND,
// INVALID_ARGUMENT).
//
// Pass [SeqCutoffSawNothing] (-1) as cutoff when trying to write
// the first match — any matching event will conflict.
//
// Exactly one of (committed, conflict) is non-nil iff err is nil.
func (c *Client) Append(
	ctx context.Context,
	filter TagFilter,
	cutoff int64,
	events []ProposedEvent,
	opts ...grpc.CallOption,
) (committed *Committed, conflict *Conflict, err error) {
	if filter == nil {
		return nil, nil, errors.New("dcb: nil filter")
	}
	pe := make([]*pb.ProposedEvent, 0, len(events))
	for _, e := range events {
		pe = append(pe, e.toProto())
	}
	resp, err := c.raw.AppendIfNoTagMatches(ctx, &pb.AppendIfNoTagMatchesRequest{
		StoreId:    c.store,
		TagFilter:  filter.toProto(),
		SeqCutoff:  cutoff,
		Events:     pe,
	}, opts...)
	if err != nil {
		return nil, nil, err
	}
	switch r := resp.GetResult().(type) {
	case *pb.AppendIfNoTagMatchesResponse_Committed:
		return &Committed{LastSeq: r.Committed.GetLastSeq()}, nil, nil
	case *pb.AppendIfNoTagMatchesResponse_Conflict:
		return nil, &Conflict{MaxSeq: r.Conflict.GetMaxSeq()}, nil
	default:
		return nil, nil, errors.New("dcb: unknown result variant")
	}
}

// Read returns events from the DCB pseudo-stream matching filter,
// ordered by seq ascending, with the highest observed seq in
// [ReadResult.MaxSeq]. batchSize is a server-side cap on returned
// events; pass 0 to let the server choose.
//
// Use ReadResult.MaxSeq as the cutoff for a subsequent Append in the
// canonical Decision loop.
func (c *Client) Read(
	ctx context.Context,
	filter TagFilter,
	batchSize uint64,
	opts ...grpc.CallOption,
) (ReadResult, error) {
	if filter == nil {
		return ReadResult{}, errors.New("dcb: nil filter")
	}
	resp, err := c.raw.ReadDcbContext(ctx, &pb.ReadDcbContextRequest{
		StoreId:    c.store,
		TagFilter:  filter.toProto(),
		BatchSize:  batchSize,
	}, opts...)
	if err != nil {
		return ReadResult{}, err
	}
	return ReadResult{
		Events: recordedSliceFromProto(resp.GetEvents()),
		MaxSeq: resp.GetMaxSeq(),
	}, nil
}

func recordedSliceFromProto(ps []*pb.RecordedEvent) []streams.RecordedEvent {
	out := make([]streams.RecordedEvent, 0, len(ps))
	for _, p := range ps {
		out = append(out, streams.RecordedEventFromProto(p))
	}
	return out
}
