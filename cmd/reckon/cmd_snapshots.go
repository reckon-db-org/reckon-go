package main

import (
	"context"
	"io"

	"github.com/reckon-db-org/reckon-go/cmd/reckon/encode"
	"github.com/reckon-db-org/reckon-go/snapshots"
)

func snapshotsResult(out io.Writer, recs []snapshots.Record, mode encode.Bytes, pretty bool) error {
	arr := make([]map[string]any, len(recs))
	for i, r := range recs {
		arr[i] = encode.Snapshot(r, mode)
	}
	return writeJSON(out, arr, pretty)
}

// reckon snapshots list <source-uuid> <stream-uuid> -> [Snapshot]
func snapshotsList(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 2 {
		return usageErr("snapshots list: expected <source-uuid> <stream-uuid>")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	recs, err := c.Snapshots(o.store).List(rctx, args[0], args[1])
	if err != nil {
		return fromGRPC(err)
	}
	return snapshotsResult(out, recs, o.bytes, o.pretty)
}

// reckon snapshots list-all -> [Snapshot]
func snapshotsListAll(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
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
	recs, err := c.Snapshots(o.store).ListAll(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	return snapshotsResult(out, recs, o.bytes, o.pretty)
}

// reckon snapshots at <source-uuid> <stream-uuid> <version> -> Snapshot
func snapshotsAt(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 3 {
		return usageErr("snapshots at: expected <source-uuid> <stream-uuid> <version>")
	}
	v, perr := parseU64(args[2])
	if perr != nil {
		return usageErr("snapshots at: invalid <version>: %v", perr)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	rec, err := c.Snapshots(o.store).At(rctx, args[0], args[1], v)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.Snapshot(rec, o.bytes), o.pretty)
}

// reckon snapshots latest <source-uuid> <stream-uuid> -> Snapshot
func snapshotsLatest(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 2 {
		return usageErr("snapshots latest: expected <source-uuid> <stream-uuid>")
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	rec, err := c.Snapshots(o.store).Latest(rctx, args[0], args[1])
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.Snapshot(rec, o.bytes), o.pretty)
}

// reckon snapshots save <source-uuid> <stream-uuid> <version> (data on stdin) -> {"saved": true}
func snapshotsSave(ctx context.Context, o *opts, args []string, in io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 3 {
		return usageErr("snapshots save: expected <source-uuid> <stream-uuid> <version>")
	}
	v, perr := parseU64(args[2])
	if perr != nil {
		return usageErr("snapshots save: invalid <version>: %v", perr)
	}
	data, ierr := io.ReadAll(in)
	if ierr != nil {
		return usageErr("snapshots save: reading data from stdin: %v", ierr)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	if err := c.Snapshots(o.store).Save(rctx, snapshots.Spec{
		SourceUUID: args[0], StreamUUID: args[1], Version: v, Data: data,
	}); err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"saved": true}, o.pretty)
}

// reckon snapshots delete <source-uuid> <stream-uuid> <version> -> {"deleted": true}
func snapshotsDelete(ctx context.Context, o *opts, args []string, _ io.Reader, out io.Writer) error {
	if err := o.requireStore(); err != nil {
		return err
	}
	if len(args) != 3 {
		return usageErr("snapshots delete: expected <source-uuid> <stream-uuid> <version>")
	}
	v, perr := parseU64(args[2])
	if perr != nil {
		return usageErr("snapshots delete: invalid <version>: %v", perr)
	}
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	if err := c.Snapshots(o.store).Delete(rctx, args[0], args[1], v); err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"deleted": true}, o.pretty)
}
