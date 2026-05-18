# Changelog

All notable changes to `reckon-go` will be documented in this file.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning: [SemVer](https://semver.org/) at the Go-API level.

## [Unreleased]

## [0.2.0] - 2026-05-18

Wraps the remaining five gateway services to round out coverage.

### Added

- `health` package — `Check`, `Health`, `ClusterConsistency`,
  `MembershipConsensus`, `RaftLogConsistency`, `MemoryLevel`,
  `MemoryStats`, `ServerInfo`. Typed Status / ClusterStatus /
  MemoryLevel enums.
- `schema` package — `Register`, `Unregister`, `Get`, `List`,
  `Version`, `Upcast`.
- `temporal` package — `Until`, `Range`, `VersionAt`. Wall-clock
  queries that return `streams.RecordedEvent` slices.
- `causation` package — `Effects`, `Cause`, `Chain`, `Correlated`,
  `Graph`. Walks event lineage by `causation_id` / `correlation_id`.
- `admin` package — store/stream/event-type stats + scavenge
  (real, dry-run, by pattern) + projection link lifecycle
  (Create / Delete / Get / List / Start / Stop / Info).
- Facades on `reckon.Client`: `Health(...)`, `Schema(...)`,
  `Temporal(...)`, `Causation(...)`, `Admin(...)`.

### Notes

- All wrappers store-bound via `reckon.Client.<Service>(storeID)`;
  one Client per store. `health.Client.Health(...)` is the only
  exception — it returns the gateway-wide snapshot regardless of
  binding.
- `temporal` / `causation` / `schema.Upcast` re-use
  `streams.RecordedEvent` rather than redefine it. Each package
  defines its own private converters; no exported helper.

## [0.1.0] - 2026-05-17

First tagged release. Top-level `reckon.Client` plus idiomatic Go
wrappers for the four primary services: `Stores`, `Streams`,
`Subscriptions`, `Snapshots`.

### Added

- `reckon.Client` facade with `Connect` / `Close` / `Conn`
- `stores` package — `List`, `Get`, `Watch` (server-streaming
  topology changes as a `<-chan Event`)
- `streams` package — `Append`, `Read`, `ReadBackward`, `Watch`
  (server-streaming events forward), `Version`, `List`, `Delete`,
  `ReadByEventTypes`, `ReadByTags`, `ReadAllGlobal`. `ExpectedVersion`
  constants for optimistic concurrency.
- `subscriptions` package — `Subscribe` (server-streaming deliveries
  as `<-chan Delivery`), `Ack`, `Create`, `Remove`, `List`, `Get`,
  `Lag`
- `snapshots` package — `Save`, `At`, `Latest` (client-side List+At
  composition since the gRPC surface has no read-latest RPC),
  `Delete`, `List`, `ListAll`
- `examples/quickstart`, `examples/streams-demo`,
  `examples/subscriptions-demo`, `examples/snapshots-demo` —
  end-to-end smoke tests against the live beam cluster
- `cmd/probe` — debug RPC reachability tool (not part of the public API)

### Notes

- Stream ids follow `{prefix}-{uuidv7-without-dashes}`, e.g.
  `users-018f6a7b8c9d4abc8901234567890abc`. The wrapper does not
  enforce this; it's the project convention.
- `Subscriptions.Lag` may return `Internal` for newly-created
  subscriptions on `reckon-gateway < 0.4.6` — fixed in 0.4.6 /
  reckon-db 2.3.2.
- `Create` followed by `Subscribe` for the same name leaves the
  original `pid=undefined` registration in place (gateway's
  `save_subscription` treats the second call as `already_exists` and
  silently drops the new pid). Subscribe alone is the supported
  path for live delivery.

### Corrected from earlier note

An earlier revision of this CHANGELOG claimed `by_stream` required
a `$`-separated stream id. That was a misdiagnosis: the gateway's
`reckon_db_filters:by_stream/1` rejected non-`$` ids with
`{invalid_filter, invalid_stream}`, which was an over-restrictive
check the *filter* imposed, not a real stream-id format requirement.
Fixed in reckon-db 2.3.2 / reckon-gateway 0.4.6 — plain
`{prefix}-{uuidv7}` ids are now accepted everywhere.

Compatible with `reckon-proto 0.2.x` / `reckon-gateway 0.4.x` / `reckon-db 2.3.x`.
