# Changelog

All notable changes to `reckon-go` will be documented in this file.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning: [SemVer](https://semver.org/) at the Go-API level.

## [Unreleased]

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

- Stream subscriptions require a `$`-separated stream id (`category$id`).
  reckon-db's `by_stream` filter rejects plain stream ids with
  `{invalid_filter, invalid_stream}`. This is a gateway/storage
  convention; the wrapper does not enforce it.
- `Subscriptions.Lag` may return `Internal` for newly-created
  subscriptions — known gateway-side issue, tracked separately.
- `Create` followed by `Subscribe` for the same name leaves the
  original `pid=undefined` registration in place (gateway's
  `save_subscription` treats the second call as `already_exists` and
  silently drops the new pid). Subscribe alone is the supported
  path for live delivery.

Compatible with `reckon-proto 0.2.x` / `reckon-gateway 0.4.x` / `reckon-db 2.3.x`.
