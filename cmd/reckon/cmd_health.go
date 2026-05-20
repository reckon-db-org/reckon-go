package main

import (
	"context"
	"io"

	"codeberg.org/reckon-db-org/reckon-go/cmd/reckon/encode"
	"codeberg.org/reckon-db-org/reckon-go/health"
)

// reckon health check (store-bound) -> CheckResult
func healthCheck(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
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
	r, err := c.Health(o.store).Check(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.CheckResult(r), o.pretty)
}

// reckon health status (gateway-wide) -> HealthResult
func healthStatus(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	r, err := c.Health("").Health(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.HealthResult(r), o.pretty)
}

// clusterCheck runs a ClusterResult check. In catalogue mode these route per
// store, so a --store is required (the gateway returns InvalidArgument for an
// empty store_id).
func clusterCheck(ctx context.Context, o *opts, out io.Writer, name string,
	fn func(*health.Client, context.Context) (health.ClusterResult, error)) error {
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
	r, err := fn(c.Health(o.store), rctx)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.ClusterResult(r), o.pretty)
}

// reckon health cluster-consistency (gateway-wide) -> ClusterResult
func healthClusterConsistency(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
	return clusterCheck(ctx, o, out, "cluster-consistency",
		func(h *health.Client, rc context.Context) (health.ClusterResult, error) {
			return h.ClusterConsistency(rc)
		})
}

// reckon health membership-consensus (gateway-wide) -> ClusterResult
func healthMembershipConsensus(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
	return clusterCheck(ctx, o, out, "membership-consensus",
		func(h *health.Client, rc context.Context) (health.ClusterResult, error) {
			return h.MembershipConsensus(rc)
		})
}

// reckon health raft-log (gateway-wide) -> ClusterResult
func healthRaftLog(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
	return clusterCheck(ctx, o, out, "raft-log",
		func(h *health.Client, rc context.Context) (health.ClusterResult, error) {
			return h.RaftLogConsistency(rc)
		})
}

// reckon health memory-level (store-bound) -> {"level": ...}
func healthMemoryLevel(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
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
	lvl, err := c.Health(o.store).MemoryLevel(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, map[string]any{"level": string(lvl)}, o.pretty)
}

// reckon health memory-stats (store-bound) -> MemoryStats
func healthMemoryStats(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
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
	m, err := c.Health(o.store).MemoryStats(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.MemoryStats(m), o.pretty)
}

// reckon health server-info (store-bound) -> ServerInfo
//
// In catalogue mode the gateway routes server-info to the BEAM owning the
// store, so a --store is required (empty store_id -> InvalidArgument).
func healthServerInfo(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
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
	s, err := c.Health(o.store).ServerInfo(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.ServerInfo(s), o.pretty)
}
