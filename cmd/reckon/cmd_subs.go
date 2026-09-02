package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"io"

	"github.com/reckon-db-org/reckon-go/cmd/reckon/encode"
	"github.com/reckon-db-org/reckon-go/subscriptions"
)

// reckon subs list -> [SubInfo]
func subsList(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
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
	infos, err := c.Subscriptions(o.store).List(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	arr := make([]map[string]any, len(infos))
	for i, in := range infos {
		arr[i] = encode.SubInfo(in)
	}
	return writeJSON(out, arr, o.pretty)
}

// reckon subs get <name> -> SubInfo
func subsGet(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("subs get: expected exactly one <name> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	info, err := c.Subscriptions(o.store).Get(rctx, args[0])
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.SubInfo(info), o.pretty)
}

// reckon subs lag <name> -> Lag
func subsLag(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 1 {
		return usageErr("subs lag: expected exactly one <name> argument")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	lag, err := c.Subscriptions(o.store).Lag(rctx, args[0])
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.Lag(lag), o.pretty)
}

// reckon subs create <name> --type T --selector SEL [--from N] [--pool K] -> {"subscription_id"}
func subsCreate(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("subs create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	typ := fs.String("type", "", "stream|event_type|event_pattern|event_payload|tags")
	selector := fs.String("selector", "", "subscription selector")
	from := fs.Uint64("from", 0, "start checkpoint")
	pool := fs.Uint("pool", 1, "consumer pool size")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("subs create: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("subs create: expected exactly one <name> argument")
	}
	st, terr := parseSubType(*typ)
	if terr != nil {
		return usageErr("subs create: %v", terr)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	id, err := c.Subscriptions(o.store).Create(rctx, subscriptions.Spec{
		Type: st, Selector: *selector, Name: rest[0], StartFrom: *from, PoolSize: uint32(*pool),
	})
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"subscription_id": id}, o.pretty)
}

// reckon subs remove <name> --type T --selector SEL -> {"removed": true}
func subsRemove(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("subs remove", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	typ := fs.String("type", "", "subscription type")
	selector := fs.String("selector", "", "subscription selector")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("subs remove: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("subs remove: expected exactly one <name> argument")
	}
	st, terr := parseSubType(*typ)
	if terr != nil {
		return usageErr("subs remove: %v", terr)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	if err := c.Subscriptions(o.store).Remove(rctx, subscriptions.Spec{
		Type: st, Selector: *selector, Name: rest[0],
	}); err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"removed": true}, o.pretty)
}

// reckon subs ack <stream> <name> <version> -> {"acked": true}
func subsAck(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 3 {
		return usageErr("subs ack: expected <stream> <name> <version>")
	}
	v, perr := parseU64(args[2])
	if perr != nil {
		return usageErr("subs ack: invalid <version>: %v", perr)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	if err := c.Subscriptions(o.store).Ack(rctx, args[0], args[1], v); err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"acked": true}, o.pretty)
}

// reckon subs consume <name> --type T --selector SEL [--from N] [--pool K]
//
//	[--ack-mode auto|none|stdin] -> NDJSON delivery frames
func subsConsume(ctx context.Context, o *opts, args []string, in io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	fs := flag.NewFlagSet("subs consume", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	typ := fs.String("type", "", "subscription type")
	selector := fs.String("selector", "", "subscription selector")
	from := fs.Uint64("from", 0, "start checkpoint")
	pool := fs.Uint("pool", 1, "consumer pool size")
	ackMode := fs.String("ack-mode", "auto", "auto|none|stdin")
	rest, err := flagsThenArgs(fs, args)
	if err != nil {
		return usageErr("subs consume: %v", err)
	}
	if len(rest) != 1 {
		return usageErr("subs consume: expected exactly one <name> argument")
	}
	st, terr := parseSubType(*typ)
	if terr != nil {
		return usageErr("subs consume: %v", terr)
	}
	if *ackMode != "auto" && *ackMode != "none" && *ackMode != "stdin" {
		return usageErr("subs consume: invalid --ack-mode %q (want auto|none|stdin)", *ackMode)
	}

	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	sc := c.Subscriptions(o.store)
	name := rest[0]

	// stdin ack mode: a reader goroutine acks {"ack":N,"stream":S} lines and
	// cancels the stream on stdin EOF (graceful shutdown).
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if *ackMode == "stdin" {
		go consumeStdinAcks(cctx, sc, name, *selector, in, cancel)
	}

	deliveries, errs := sc.Subscribe(cctx, subscriptions.Spec{
		Type: st, Selector: *selector, Name: name, StartFrom: *from, PoolSize: uint32(*pool),
	})
	return pump(cctx, out, deliveries, errs, func(d subscriptions.Delivery) map[string]any {
		if *ackMode == "auto" {
			_ = sc.Ack(cctx, d.Event.StreamID, name, d.Checkpoint)
		}
		return encode.Delivery(d, o.bytes)
	})
}

// consumeStdinAcks reads {"ack":N,"stream":S} lines and acks them; stream
// defaults to the subscription selector. Calls cancel on stdin EOF.
func consumeStdinAcks(ctx context.Context, sc *subscriptions.Client, name, selector string, in io.Reader, cancel context.CancelFunc) {
	defer cancel()
	scan := bufio.NewScanner(in)
	for scan.Scan() {
		var m struct {
			Ack    uint64 `json:"ack"`
			Stream string `json:"stream"`
		}
		if json.Unmarshal(scan.Bytes(), &m) != nil {
			continue
		}
		stream := m.Stream
		if stream == "" {
			stream = selector
		}
		_ = sc.Ack(ctx, stream, name, m.Ack)
	}
}
