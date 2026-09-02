package main

import (
	"context"
	"io"

	"github.com/reckon-db-org/reckon-go/cmd/reckon/encode"
)

// reckon schema list -> [SchemaDef]
func schemaList(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
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
	defs, err := c.Schema(o.store).List(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	arr := make([]map[string]any, len(defs))
	for i, d := range defs {
		arr[i] = encode.SchemaDef(d, o.bytes)
	}
	return writeJSON(out, arr, o.pretty)
}

// reckon schema get <event-type> -> SchemaDef
func schemaGet(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("schema get: expected exactly one <event-type> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	def, err := c.Schema(o.store).Get(rctx, args[0])
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.SchemaDef(def, o.bytes), o.pretty)
}

// reckon schema version <event-type> -> {"version": uint32}
func schemaVersion(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("schema version: expected exactly one <event-type> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	v, err := c.Schema(o.store).Version(rctx, args[0])
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"version": v}, o.pretty)
}

// reckon schema register <event-type> (schema blob on stdin) -> {"registered": true}
func schemaRegister(ctx context.Context, o *opts, args []string, in io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("schema register: expected exactly one <event-type> argument")
	}
	blob, ierr := io.ReadAll(in)
	if ierr != nil {
		return usageErr("schema register: reading schema from stdin: %v", ierr)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	if err := c.Schema(o.store).Register(rctx, args[0], blob); err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"registered": true}, o.pretty)
}

// reckon schema unregister <event-type> -> {"unregistered": true}
func schemaUnregister(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("schema unregister: expected exactly one <event-type> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	if err := c.Schema(o.store).Unregister(rctx, args[0]); err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"unregistered": true}, o.pretty)
}

// reckon schema upcast (events on stdin) -> [Event]
func schemaUpcast(ctx context.Context, o *opts, _ []string, in io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	events, ierr := readProposedEvents(in)
	if ierr != nil {
		return usageErr("schema upcast: reading events from stdin: %v", ierr)
	}
	// Upcast operates on recorded events; reuse the proposed-event input shape
	// to carry event_type + data, mapping to a minimal RecordedEvent.
	recs := proposedToRecorded(events)

	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	upcasted, err := c.Schema(o.store).Upcast(rctx, recs)
	if err != nil {
		return fromGRPC(err)
	}
	return eventsResult(out, upcasted, o.bytes, o.pretty)
}
