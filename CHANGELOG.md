# Changelog

All notable changes to `reckon-go` will be documented in this file.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning: [SemVer](https://semver.org/) at the Go-API level.

## [0.7.0] - 2026-06-10

### Security — TLS is the default transport (BREAKING for plaintext gateways)

Fixes the 2026-06-10 audit finding: `Connect` silently injected
`insecure.NewCredentials()` when no DialOptions were given, so every
SDK consumer and the whole CLI ran plaintext, unauthenticated gRPC —
including destructive admin RPCs — with no way to enable TLS.

- **SDK:** `Connect` with no DialOptions now uses TLS verified
  against the system root pool. Plaintext is an explicit opt-in:
  `reckon.Connect(ctx, ep, reckon.Insecure())`. New helpers:
  `reckon.TLSWithCA(caFile, serverName)` (self-signed / private-CA
  gateways) and `reckon.TLSWithServerName(name)` (dial by IP, verify
  by DNS name). Callers passing their own DialOptions are unaffected.
- **CLI:** new global flags `--plaintext` (env `RECKON_PLAINTEXT=1`),
  `--ca` (env `RECKON_CA`), `--server-name` (env
  `RECKON_SERVER_NAME`). Default is TLS + system roots;
  `--plaintext` conflicts with the TLS flags.

**Migration:** today's lab gateways (`beamNN:50051`) serve plaintext
gRPC, so existing invocations need `--plaintext` (CLI) or
`reckon.Insecure()` (SDK) until the gateway grows TLS. Dialing a
plaintext gateway with the new default fails loudly with a TLS
handshake error instead of silently downgrading.

## [0.6.0] - 2026-06-08

### Added — `(*streams.Client).ReadByMetadata`

```go
events, err := streams.ReadByMetadata(ctx, "causation_id", "evt-7", 0)
```

Reads events whose metadata `key == value` across all streams — the
cross-cutting primitive for application-built causation/correlation read
models. O(matches) when the store declared the `{meta, key}` secondary
index (reckon-db 5.0.0+), else a server-side scan; the store does not
interpret the key. Mirrors `ReadByTags` / `ReadByEventTypes`. Stubs
regenerated from reckon-proto 0.6.0 (`StreamService.ReadByMetadata`).

### Added — lineage convenience (`streams/lineage.go`)

Convention sugar over `ReadByMetadata`, so Go callers don't hand-roll the
Enterprise Integration Patterns keys:

- Reserved-key constants `CausationIDKey` / `CorrelationIDKey` /
  `ConversationIDKey` (matching reckon_shared.proto, the single source of
  truth for the names).
- `ReadEffects(messageID)` / `ReadCorrelated(correlationID)` /
  `ReadConversation(conversationID)` — named readers over the reserved keys.
- `WithLineage(meta, causationID, correlationID)` for building a
  `ProposedEvent.Metadata` map before marshalling.

These are convenience, not a store feature, and explicitly NOT
auto-propagation (a raw client has no notion of "the message I am handling";
propagation is a framework's job, e.g. evoq on BEAM).

### Removed — causation package (BREAKING)

Deleted the `causation` package (`Client.Causation()`, `Effects` /
`Cause` / `Chain` / `Correlated` / `Graph`, `GraphNode`), the
`reckon causation …` CLI subcommands, and the generated
`reckon_causation*.pb.go` stubs — tracking reckon-proto 0.5.0, which
dropped `CausationService`.

Causation/correlation traversal is not an event-store concern.
`causation_id` and `correlation_id` are still ordinary keys in an
event's metadata (`RecordedEvent.Metadata`); the store relays metadata
verbatim. Consumers that need lineage build a read model.

### Added

- `scripts/release-local.sh` — one-command manual release (build + publish to
  Codeberg), the working release path while the GitHub Actions/mirror route is
  blocked (see `plans/DESIGN_RECKON_CLI.md` §7a).

### Fixed

- README install instructions reflect that the repo is public (no token needed
  for `install.sh`); the token path is documented only as a private-repo
  fallback.

## [0.4.0] - 2026-05-20

### Added

- Prebuilt-binary distribution so the `reckon` CLI installs without a Go
  toolchain. `scripts/install.sh` (`curl | sh`) downloads the matching binary
  from Codeberg releases, verifies SHA256, and installs to `~/.local/bin`
  (`RECKON_VERSION`/`RECKON_BIN_DIR`/`RECKON_TOKEN` env). Binaries are built by
  `scripts/build-release.sh` (5 platforms, stripped + version-stamped) and
  published to Codeberg releases by `scripts/publish-codeberg.sh`, driven by
  `.github/workflows/release.yml` on every `v*` tag (needs the GitHub secret
  `CODEBERG_TOKEN`). Codeberg releases remain canonical; GitHub is only the
  Actions runner. See `plans/DESIGN_RECKON_CLI.md` §7a.
- `reckon --version` / `-V` prints `{client, api_compat}`; the client version
  is stamped into release binaries at build time.

## [0.3.0] - 2026-05-20

### Fixed

- `Connect` now dials bare `host:port` targets through the `passthrough`
  resolver instead of the default `dns` resolver. The dns resolver issues
  synthetic `_grpc_config.<host>` (TXT) and `_grpclb._tcp.<host>` (SRV)
  service-config lookups; on private TLDs whose nameservers don't answer
  them (e.g. `*.lab`), the first RPC hangs until its deadline — every call
  to a `.lab` gateway timed out with `DeadlineExceeded` despite the gateway
  being healthy and TCP-reachable. Passthrough delegates resolution to the
  dialer (honouring `/etc/hosts`/nsswitch). Pass an explicit scheme
  (`dns:///host:port`) to opt back into the dns resolver / client-side LB.

### Added

- `cmd/reckon`: a single static binary exposing the reckon-go API as
  JSON-emitting subcommands (NDJSON for streams), for reckon-nvim and
  shell/CI use. See `plans/DESIGN_RECKON_CLI.md`. The full surface is wired
  across all groups — stores, streams, subs, snapshots, schema, temporal,
  causation, admin (incl. links), health, catalogue (54 leaf commands).
  Streaming commands (`stores watch`, `streams watch`, `subs consume`) emit
  NDJSON frames; `subs consume` supports `--ack-mode auto|none|stdin` (stdin
  acks for at-least-once UIs). Stdlib `flag` only — no third-party deps leak
  into the module graph. Verified end-to-end against a live gateway cluster
  (e2e tests behind the `e2e` build tag).
- `admin.ReloadCatalogue(ctx)` and `admin.GetCatalogueStatus(ctx)`
  wrap the two catalogue-mode RPCs introduced in reckon-gateway 0.5+
  / reckon-proto 0.3.0 (rename-fixed in 0.3.1). Both are gateway-wide;
  the bound store_id is ignored. Returns typed `CatalogueReloadResult`
  / `CatalogueStatus` / `CatalogueClusterInfo` records.
- Stubs regenerated from reckon-proto v0.3.1 (`CatalogueClusterStatus`
  message renamed from `ClusterStatus` to avoid a namespace clash
  with the `enum ClusterStatus` in reckon_health.proto).
- Unit tests for all 9 service packages (admin, causation, health,
  schema, snapshots, stores, streams, subscriptions, temporal). Uses
  `google.golang.org/grpc/test/bufconn` for an in-process gRPC server
  so the tests exercise the real wire encode/decode + typed-result
  translation without needing a live gateway. Covers happy paths,
  RPC error propagation, enum/timestamp mapping, server-streaming
  delivery, and nil-safety on optional proto fields.

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
