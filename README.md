# reckon-go
[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-support-yellow.svg)](https://buymeacoffee.com/rlefever)

Idiomatic Go client for the [ReckonDB](https://github.com/reckon-db-org/reckon-db) event store, accessed over gRPC via the [reckon-gateway](https://github.com/reckon-db-org/reckon-gateway) frontend.

```go
import "github.com/reckon-db-org/reckon-go"

c, err := reckon.Connect(ctx, "gateway.example.org:50051") // TLS, system roots (the default)
if err != nil { ... }
defer c.Close()

stores, err := c.Stores().List(ctx)
```

## What it is

`reckon-go` is the Go client for the Reckon event-sourcing stack. It speaks
gRPC to a running [reckon-gateway](https://github.com/reckon-db-org/reckon-gateway),
which fronts a [reckon-db](https://github.com/reckon-db-org/reckon-db) event
store (embedded in the gateway, or federated across remote Erlang clusters).
You get typed, idiomatic Go over the full gateway surface without speaking
Erlang dist or hand-rolling protobuf.

The package import path ends in `reckon-go`, but the Go package name is
`reckon` (so calls read `reckon.Connect(...)`). One `reckon.Client` wraps one
gRPC connection to one gateway endpoint; per-service sub-clients are bound to a
store and share that connection:

| Sub-client | Method | Purpose |
|---|---|---|
| `stores` | `c.Stores()` | cluster topology discovery + watch |
| `streams` | `c.Streams(store)` | append + read events on a stream |
| `subscriptions` | `c.Subscriptions(store)` | live + persistent subscriptions |
| `snapshots` | `c.Snapshots(store)` | per-stream snapshots |
| `dcb` | `c.Dcb(store)` | Dynamic Consistency Boundary writes/reads |
| `schema` | `c.Schema(store)` | schema registration + upcasting |
| `temporal` | `c.Temporal(store)` | wall-clock / time-travel reads |
| `admin` | `c.Admin(store)` | scavenge, projection links, store stats |
| `health` | `c.Health()` | gateway-wide gRPC health snapshot |

Most sub-clients take a `store` id and are cheap to construct (they reuse the
underlying connection). `Stores()` and `Health()` are gateway-wide and take no
store.

## Install

As a library:

```bash
go get github.com/reckon-db-org/reckon-go
```

```go
import (
    reckon "github.com/reckon-db-org/reckon-go"
    "github.com/reckon-db-org/reckon-go/streams"
)
```

For the `reckon` CLI binary, see [`reckon` CLI](#reckon-cli) below (prebuilt
binary or `go install`).

## Quick start: connect, append, read

A full round-trip against a lab gateway (plaintext): append three events to a
stream, then read them back forward from version 0.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    reckon "github.com/reckon-db-org/reckon-go"
    "github.com/reckon-db-org/reckon-go/streams"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()

    // Lab gateway over plaintext gRPC. Drop reckon.Insecure() for a
    // TLS gateway (the default), or use reckon.TLSWithCA for a private CA.
    c, err := reckon.Connect(ctx, "beam01.lab:50051", reckon.Insecure())
    if err != nil {
        log.Fatalf("connect: %v", err)
    }
    defer c.Close()

    s := c.Streams("default_store")

    // Append with no concurrency check. Use streams.NoStream to assert the
    // stream is new, or a non-negative version for an exact optimistic check.
    res, err := s.Append(ctx, "users-42", streams.AnyVersion, []streams.ProposedEvent{
        {EventType: "user_registered_v1", Data: []byte(`{"name":"Ada"}`)},
        {EventType: "user_promoted_v1", Data: []byte(`{"role":"admin"}`)},
    })
    if err != nil {
        log.Fatalf("append: %v", err)
    }
    fmt.Printf("appended: version=%d position=%d count=%d\n",
        res.Version, res.Position, res.Count)

    // Read forward from version 0, up to 100 events.
    events, err := s.Read(ctx, "users-42", 0, 100)
    if err != nil {
        log.Fatalf("read: %v", err)
    }
    for _, e := range events {
        fmt.Printf("  v=%-3d %-24s %s\n", e.Version, e.EventType, string(e.Data))
    }
}
```

`streams.AnyVersion`, `streams.NoStream`, and `streams.StreamExists` are the
optimistic-concurrency sentinels on `Append`; pass a non-negative
`streams.ExpectedVersion` for an exact-version check. `Append` returns an
`AppendResult{Version, Position, Count}`; `Read` returns `[]streams.RecordedEvent`.
For a live tail use `s.Watch(ctx, streamID, startVersion, count)`, which returns
an event channel plus an error channel.

Runnable variants of this live under [`examples/`](examples/) (streams,
subscriptions, snapshots, admin, health), wired against the beam cluster.

## Dynamic consistency: DCB and CCC

For cross-cutting invariants that do not live inside a single aggregate
(uniqueness, allocation, rate-limit, eligibility), use the **DCB** (Dynamic
Consistency Boundary) sub-client instead of per-stream optimistic concurrency:

```go
d := c.Dcb("orders")

// Read the consistency context: events matching a tag filter, plus the
// highest seq observed (MaxSeq).
rr, err := d.Read(ctx, dcb.MatchAny("slot:42"), 100)

// ...domain logic decides...

// Conditional append: commits iff no event matching the filter has a seq
// strictly above the cutoff. A non-nil *Conflict means "context stale, retry".
committed, conflict, err := d.Append(ctx, dcb.MatchAny("slot:42"), rr.MaxSeq,
    []dcb.ProposedEvent{{EventType: "slot_reserved_v1", Tags: []string{"slot:42"}}})
```

Filters compose: `dcb.MatchAny`, `dcb.MatchAll`, `dcb.EventType`, `dcb.And`,
`dcb.Or`. `Append` returns `(committed *Committed, conflict *Conflict, err error)`
(exactly one of `committed`/`conflict` is non-nil when `err` is nil), which is
the canonical read-decide-append-retry loop (see the `dcb` package doc).

The DCB client also exposes **CCC** (Command Context Consistency) reads keyed
by JSON payload field values rather than tags:

```go
// Events where data["account_id"] == "acc-42":
events, err := d.CccReadByPayload(ctx, "account_id", "acc-42", 100)

// Events where data["flight_id"] == "FL-001" AND data["seat_no"] == "14A":
events, err := d.CccReadByPayloadHash(ctx,
    []string{"flight_id", "seat_no"},
    []string{"FL-001", "14A"},
    100,
)
```

Both return `[]streams.RecordedEvent`. They need a `{ccc, key}` (single field)
or `{ccc_hash, keys}` (multi-field) index declared in the store config, backed
by reckon-db 5.4.0+ / reckon-gateway 0.13.1+.

## Compatibility

`reckon-go` is generated against [reckon-proto](https://github.com/reckon-db-org/reckon-proto)
and connects to a running [reckon-gateway](https://github.com/reckon-db-org/reckon-gateway).
The committed gRPC stubs track reckon-proto minor versions; any gateway
speaking the same proto minor is compatible. Some methods need a minimum
backing store:

| Feature | Minimum backing |
|---|---|
| Core streams / subscriptions / snapshots | reckon-gateway 0.4.x, reckon-db 2.3.x |
| `streams.ReadByMetadata` (`{meta, key}` index) | reckon-db 5.0.0+ |
| `dcb.EventType` filter (`[by_event_type]` index) | reckon-db 5.2.0+ |
| `dcb.CccReadByPayload` / `CccReadByPayloadHash` | reckon-db 5.4.0+, reckon-gateway 0.13.1+ |

See [Versioning](#versioning) for the pinning convention and CHANGELOG.md for
the per-release proto mapping.

## Transport security

Since 0.7.0 `Connect` defaults to TLS verified against the system root
pool. Plaintext is an explicit opt-in, never a silent default:

```go
// Lab gateway without TLS (e.g. beam01.lab:50051):
c, err := reckon.Connect(ctx, "beam01.lab:50051", reckon.Insecure())

// Self-signed / private-CA gateway:
opt, err := reckon.TLSWithCA("/etc/reckon/ca.pem", "")
c, err := reckon.Connect(ctx, "gateway.internal:50051", opt)
```

The CLI mirrors this: TLS by default, `--plaintext` for lab gateways,
`--ca` / `--server-name` for private trust anchors.

## Status

All nine gateway services wrapped. Full RPC coverage including the
catalogue-mode admin operations (`Admin.ReloadCatalogue`,
`Admin.GetCatalogueStatus`).

| Service | Wrapper status |
|---|---|
| `Stores` | ✅ |
| `Streams` | ✅ |
| `Subscriptions` | ✅ |
| `Snapshots` | ✅ |
| `Schema` | ✅ |
| `Temporal` | ✅ |
| `Admin` | ✅ |
| `Health` | ✅ |
| `Dcb` | ✅ |

## `reckon` CLI

A single static binary exposing the whole API as JSON-emitting subcommands
(NDJSON for streams), for shell/CI use and as the backend for editor
integrations like reckon-nvim. See [`plans/DESIGN_RECKON_CLI.md`](plans/DESIGN_RECKON_CLI.md).

**Install without a Go toolchain** (prebuilt binary from GitHub releases):

```bash
curl -fsSL https://raw.githubusercontent.com/reckon-db-org/reckon-go/main/scripts/install.sh | sh
```

Installs `reckon` into `~/.local/bin` (override with `RECKON_BIN_DIR`, pin a
release with `RECKON_VERSION=vX.Y.Z`). Verifies the SHA256 before installing.
(If the repo is ever made private, set `RECKON_TOKEN` to a GitHub read token;
`install.sh` then sends it as an `Authorization` header for both the script
fetch and the download.)

**With Go** (the repo is public, so this is all you need):

```bash
go install github.com/reckon-db-org/reckon-go/cmd/reckon@latest
```

<details>
<summary>If the repo is ever made private</summary>

```bash
go install github.com/reckon-db-org/reckon-go/cmd/reckon@latest
```
</details>

```bash
reckon --version
reckon -e beam01.lab:50051 -s default_store streams read user-123 --count 10
reckon -e beam01.lab:50051 -s default_store streams watch user-123   # NDJSON
```

Release binaries (`linux/{amd64,arm64}`, `darwin/{amd64,arm64}`,
`windows/amd64`) are built and published to GitHub releases by
`.github/workflows/release.yml` on every `v*` tag.

## Codegen

Generated gRPC stubs live in `genproto/gatewayv1/`, written by [buf](https://buf.build) from the canonical proto bundle in [reckon-proto](https://github.com/reckon-db-org/reckon-proto):

```bash
cd ../reckon-proto && buf generate --template buf.gen.yaml --output ../reckon-go
```

The generated code is committed (not regenerated on `go build`) so consumers don't need buf installed. Regen on every proto change.

## Versioning

`reckon-go` tracks `reckon-proto` minor versions. SemVer at the Go-API level:

- **MAJOR**: breaking client API change
- **MINOR**: new wrappers, new functionality
- **PATCH**: bug fixes, behaviour unchanged

`reckon-go 0.x.y` against `reckon-proto 0.x.*` is the pinning convention while both are pre-1.0.

## Reckon stack

reckon-go is the Go client for the Reckon event-sourcing ecosystem:

- **[reckon-proto](https://github.com/reckon-db-org/reckon-proto)**: the wire-contract protobufs this client is generated against.
- **[reckon-gateway](https://github.com/reckon-db-org/reckon-gateway)**: the gRPC + HTTP/JSON ingress this client connects to. It serves a reckon-db store (embedded or federated).
- **[reckon-gater](https://github.com/reckon-db-org/reckon-gater)**: shared types and the store-worker API behind the gateway.
- **[reckon-db](https://github.com/reckon-db-org/reckon-db)**: the BEAM-native event store the gateway fronts.
- **[evoq](https://github.com/reckon-db-org/evoq)** + **[reckon-evoq](https://github.com/reckon-db-org/reckon-evoq)**: the CQRS framework and its Reckon-store adapter (server side).
- **reckon-portal**: docs and landing site ([reckon-internal/reckon-portal](https://github.com/reckon-db-org/reckon-portal)).

## License

Apache-2.0.
