package main

import (
	"context"
	"io"

	"github.com/reckon-db-org/reckon-go/cmd/reckon/encode"
	"github.com/reckon-db-org/reckon-go/stores"
)

// reckon stores list -> [Instance]
func storesList(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	insts, err := c.Stores().List(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	arr := make([]map[string]any, len(insts))
	for i, in := range insts {
		arr[i] = encode.Instance(in)
	}
	return writeJSON(out, arr, o.pretty)
}

// reckon stores watch -> NDJSON store_event frames
func storesWatch(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	events, errs := c.Stores().Watch(ctx)
	return pump(ctx, out, events, errs, func(e stores.Event) map[string]any {
		return encode.StoreEvent(e)
	})
}
