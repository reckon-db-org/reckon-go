package main

import (
	"context"
	"flag"
	"io"
	"strings"

	"github.com/reckon-db-org/reckon-go/cmd/reckon/encode"
	"github.com/reckon-db-org/reckon-go/streams"
)

// eventsResult encodes a slice of recorded events as a JSON array.
func eventsResult(out io.Writer, evs []streams.RecordedEvent, mode encode.Bytes, pretty bool) error {
	arr := make([]map[string]any, len(evs))
	for i, e := range evs {
		arr[i] = encode.Event(e, mode)
	}
	return writeJSON(out, arr, pretty)
}

// reckon streams list -> [string]
func streamsList(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	ids, err := c.Streams(o.store).List(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	if ids == nil {
		ids = []string{}
	}
	return writeJSON(out, ids, o.pretty)
}

// reckon streams read <stream> [--from N] [--count M] [--backward] -> [Event]
func streamsRead(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("streams read", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	from := fs.Uint64("from", 0, "start version")
	count := fs.Uint64("count", 100, "max events")
	backward := fs.Bool("backward", false, "read newest-first")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("streams read: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("streams read: expected exactly one <stream> argument")
	}

	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	sc := c.Streams(o.store)
	read := sc.Read
	if *backward {
		read = sc.ReadBackward
	}
	evs, err := read(rctx, rest[0], *from, *count)
	if err != nil {
		return fromGRPC(err)
	}
	return eventsResult(out, evs, o.bytes, o.pretty)
}

// reckon streams version <stream> -> {"version": int64}
func streamsVersion(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("streams version: expected exactly one <stream> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	v, err := c.Streams(o.store).Version(rctx, args[0])
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"version": v}, o.pretty)
}

// reckon streams delete <stream> -> {"deleted": true}
func streamsDelete(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("streams delete: expected exactly one <stream> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	if err := c.Streams(o.store).Delete(rctx, args[0]); err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"deleted": true}, o.pretty)
}

// reckon streams append <stream> [--expect SPEC] (events on stdin) -> AppendResult
func streamsAppend(ctx context.Context, o *opts, args []string, in io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("streams append", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	expect := fs.String("expect", "any", "expected version: no-stream|any|exists|<N>")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("streams append: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("streams append: expected exactly one <stream> argument")
	}
	ev, perr := parseExpected(*expect)
	if perr != nil {
		return usageErr("streams append: %v", perr)
	}
	events, ierr := readProposedEvents(in)
	if ierr != nil {
		return usageErr("streams append: reading events from stdin: %v", ierr)
	}

	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	res, err := c.Streams(o.store).Append(rctx, rest[0], ev, events)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{
		"version": res.Version, "position": res.Position, "count": res.Count,
	}, o.pretty)
}

// reckon streams by-types <t1,t2,…> [--batch N] -> [Event]
func streamsByTypes(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("streams by-types", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	batch := fs.Uint64("batch", 100, "max events")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("streams by-types: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("streams by-types: expected comma-separated <types>")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	evs, err := c.Streams(o.store).ReadByEventTypes(rctx, splitCSV(rest[0]), *batch)
	if err != nil {
		return fromGRPC(err)
	}
	return eventsResult(out, evs, o.bytes, o.pretty)
}

// reckon streams by-tags <t1,t2,…> [--match any|all] [--batch N] -> [Event]
func streamsByTags(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("streams by-tags", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	match := fs.String("match", "any", "tag match: any|all")
	batch := fs.Uint64("batch", 100, "max events")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("streams by-tags: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("streams by-tags: expected comma-separated <tags>")
	}
	tm := streams.TagMatchAny
	switch *match {
	case "any", "":
	case "all":
		tm = streams.TagMatchAll
	default:
		return usageErr("streams by-tags: invalid --match %q (want any|all)", *match)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	evs, err := c.Streams(o.store).ReadByTags(rctx, splitCSV(rest[0]), tm, *batch)
	if err != nil {
		return fromGRPC(err)
	}
	return eventsResult(out, evs, o.bytes, o.pretty)
}

// reckon streams all [--offset N] [--limit M] -> [Event]
func streamsAll(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("streams all", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	offset := fs.Uint64("offset", 0, "global offset")
	limit := fs.Uint64("limit", 100, "max events")
	if _, err := flagsThenArgs(fs, args); err != nil {
		return usageErr("streams all: %v", err)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	evs, err := c.Streams(o.store).ReadAllGlobal(rctx, *offset, *limit)
	if err != nil {
		return fromGRPC(err)
	}
	return eventsResult(out, evs, o.bytes, o.pretty)
}

// reckon streams watch <stream> [--from N] [--count M] -> NDJSON event frames
func streamsWatch(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("streams watch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	from := fs.Uint64("from", 0, "start version")
	count := fs.Uint64("count", 0, "max events (0 = unbounded)")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("streams watch: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("streams watch: expected exactly one <stream> argument")
	}

	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	events, errs := c.Streams(o.store).Watch(ctx, rest[0], *from, *count)
	return pump(ctx, out, events, errs, func(e streams.RecordedEvent) map[string]any {
		return map[string]any{"type": "event", "event": encode.Event(e, o.bytes)}
	})
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
