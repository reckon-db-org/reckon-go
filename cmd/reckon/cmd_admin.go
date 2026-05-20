package main

import (
	"context"
	"flag"
	"io"

	"codeberg.org/reckon-db-org/reckon-go/admin"
	"codeberg.org/reckon-db-org/reckon-go/cmd/reckon/encode"
)

// reckon admin stats -> StoreStats
func adminStats(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
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
	s, err := c.Admin(o.store).StoreStats(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.StoreStats(s), o.pretty)
}

// reckon admin stream-info <stream> -> StreamInfo
func adminStreamInfo(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("admin stream-info: expected exactly one <stream> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	s, err := c.Admin(o.store).StreamInfo(rctx, args[0])
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.StreamInfo(s), o.pretty)
}

// reckon admin event-types -> [EventTypeCount]
func adminEventTypes(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
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
	counts, err := c.Admin(o.store).EventTypeSummary(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	arr := make([]map[string]any, len(counts))
	for i, e := range counts {
		arr[i] = encode.EventTypeCount(e)
	}
	return writeJSON(out, arr, o.pretty)
}

// reckon admin scavenge <stream> [--dry-run] [--opt k=v …] -> ScavengeResult
func adminScavenge(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("admin scavenge", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dryRun := fs.Bool("dry-run", false, "report without removing")
	options := kvFlag{}
	fs.Var(options, "opt", "scavenge option key=value (repeatable)")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("admin scavenge: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("admin scavenge: expected exactly one <stream> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	ac := c.Admin(o.store)
	run := ac.Scavenge
	if *dryRun {
		run = ac.ScavengeDryRun
	}
	res, err := run(rctx, rest[0], options)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.ScavengeResult(res), o.pretty)
}

// reckon admin scavenge-matching <pattern> [--opt k=v …] -> [ScavengeResult]
func adminScavengeMatching(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("admin scavenge-matching", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	options := kvFlag{}
	fs.Var(options, "opt", "scavenge option key=value (repeatable)")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("admin scavenge-matching: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("admin scavenge-matching: expected exactly one <pattern> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	results, err := c.Admin(o.store).ScavengeMatching(rctx, rest[0], options)
	if err != nil {
		return fromGRPC(err)
	}
	arr := make([]map[string]any, len(results))
	for i, r := range results {
		arr[i] = encode.ScavengeResult(r)
	}
	return writeJSON(out, arr, o.pretty)
}

// reckon admin links <list|get|create|delete|start|stop|info> …
func adminLinks(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) < 1 {
		return usageErr("admin links: expected <list|get|create|delete|start|stop|info> …")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	ac := c.Admin(o.store)
	sub, rest := args[0], args[1:]

	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()

	switch sub {
	case "list":
		links, err := ac.ListLinks(rctx)
		if err != nil {
			return fromGRPC(err)
		}
		arr := make([]map[string]any, len(links))
		for i, l := range links {
			arr[i] = encode.LinkSpec(l)
		}
		return writeJSON(out, arr, o.pretty)
	case "get":
		if len(rest) != 1 {
			return usageErr("admin links get: expected <name>")
		}
		l, err := ac.GetLink(rctx, rest[0])
		if err != nil {
			return fromGRPC(err)
		}
		return writeJSON(out, encode.LinkSpec(l), o.pretty)
	case "info":
		if len(rest) != 1 {
			return usageErr("admin links info: expected <name>")
		}
		l, err := ac.LinkInfo(rctx, rest[0])
		if err != nil {
			return fromGRPC(err)
		}
		return writeJSON(out, encode.LinkRuntime(l), o.pretty)
	case "create":
		return adminLinkCreate(rctx, ac, o, rest, out)
	case "delete":
		if len(rest) != 1 {
			return usageErr("admin links delete: expected <name>")
		}
		if err := ac.DeleteLink(rctx, rest[0]); err != nil {
			return fromGRPC(err)
		}
		return writeJSON(out, map[string]any{"deleted": true}, o.pretty)
	case "start":
		if len(rest) != 1 {
			return usageErr("admin links start: expected <name>")
		}
		if err := ac.StartLink(rctx, rest[0]); err != nil {
			return fromGRPC(err)
		}
		return writeJSON(out, map[string]any{"started": true}, o.pretty)
	case "stop":
		if len(rest) != 1 {
			return usageErr("admin links stop: expected <name>")
		}
		if err := ac.StopLink(rctx, rest[0]); err != nil {
			return fromGRPC(err)
		}
		return writeJSON(out, map[string]any{"stopped": true}, o.pretty)
	default:
		return usageErr("admin links: unknown subcommand %q", sub)
	}
}

func adminLinkCreate(rctx context.Context, ac *admin.Client, o *opts, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("admin links create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	source := fs.String("source", "", "source stream")
	target := fs.String("target", "", "target stream")
	options := kvFlag{}
	fs.Var(options, "opt", "link option key=value (repeatable)")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("admin links create: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("admin links create: expected <name>")
	}
	if err := ac.CreateLink(rctx, admin.LinkSpec{
		Name: rest[0], Source: *source, Target: *target, Options: options,
	}); err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"created": true}, o.pretty)
}
