package main

import (
	"context"
	"io"

	"codeberg.org/reckon-db-org/reckon-go/causation"
	"codeberg.org/reckon-db-org/reckon-go/cmd/reckon/encode"
	"codeberg.org/reckon-db-org/reckon-go/streams"
)

// causationList runs a one-arg causation query that returns a slice of events.
func causationList(ctx context.Context, o *opts, args []string, out io.Writer, name string,
	fn func(*causation.Client, context.Context, string) ([]streams.RecordedEvent, error)) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("causation %s: expected exactly one <id> argument", name)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	evs, err := fn(c.Causation(o.store), rctx, args[0])
	if err != nil {
		return fromGRPC(err)
	}
	return eventsResult(out, evs, o.bytes, o.pretty)
}

// reckon causation effects <event-id> -> [Event]
func causationEffects(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	return causationList(ctx, o, args, out, "effects",
		func(c *causation.Client, rc context.Context, id string) ([]streams.RecordedEvent, error) {
			return c.Effects(rc, id)
		})
}

// reckon causation chain <event-id> -> [Event]
func causationChain(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	return causationList(ctx, o, args, out, "chain",
		func(c *causation.Client, rc context.Context, id string) ([]streams.RecordedEvent, error) {
			return c.Chain(rc, id)
		})
}

// reckon causation correlated <correlation-id> -> [Event]
func causationCorrelated(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	return causationList(ctx, o, args, out, "correlated",
		func(c *causation.Client, rc context.Context, id string) ([]streams.RecordedEvent, error) {
			return c.Correlated(rc, id)
		})
}

// reckon causation cause <event-id> -> Event
func causationCause(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("causation cause: expected exactly one <event-id> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	ev, err := c.Causation(o.store).Cause(rctx, args[0])
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.Event(ev, o.bytes), o.pretty)
}

// reckon causation graph <event-id> -> GraphNode (recursive)
func causationGraph(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("causation graph: expected exactly one <event-id> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	node, err := c.Causation(o.store).Graph(rctx, args[0])
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.GraphNode(node, o.bytes), o.pretty)
}
