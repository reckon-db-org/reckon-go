package main

import (
	"context"
	"flag"
	"io"
)

// reckon temporal until <stream> <ts> [--batch N] -> [Event]
func temporalUntil(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("temporal until", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	batch := fs.Uint64("batch", 100, "max events")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("temporal until: %v", err)
	}
	if len(rest) != 2 {
		return usageErr("temporal until: expected <stream> <timestamp>")
	}
	cutoff, terr := parseTime(rest[1])
	if terr != nil {
		return usageErr("temporal until: %v", terr)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	evs, err := c.Temporal(o.store).Until(rctx, rest[0], cutoff, *batch)
	if err != nil {
		return fromGRPC(err)
	}
	return eventsResult(out, evs, o.bytes, o.pretty)
}

// reckon temporal range <stream> <from-ts> <to-ts> [--batch N] -> [Event]
func temporalRange(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("temporal range", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	batch := fs.Uint64("batch", 100, "max events")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("temporal range: %v", err)
	}
	if len(rest) != 3 {
		return usageErr("temporal range: expected <stream> <from-ts> <to-ts>")
	}
	from, ferr := parseTime(rest[1])
	if ferr != nil {
		return usageErr("temporal range: from: %v", ferr)
	}
	to, terr := parseTime(rest[2])
	if terr != nil {
		return usageErr("temporal range: to: %v", terr)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	evs, err := c.Temporal(o.store).Range(rctx, rest[0], from, to, *batch)
	if err != nil {
		return fromGRPC(err)
	}
	return eventsResult(out, evs, o.bytes, o.pretty)
}

// reckon temporal version-at <stream> <ts> -> {"version": int64}
func temporalVersionAt(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 2 {
		return usageErr("temporal version-at: expected <stream> <timestamp>")
	}
	at, terr := parseTime(args[1])
	if terr != nil {
		return usageErr("temporal version-at: %v", terr)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	v, err := c.Temporal(o.store).VersionAt(rctx, args[0], at)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"version": v}, o.pretty)
}
