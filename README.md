# reckon-go

Idiomatic Go client for the [ReckonDB](https://codeberg.org/reckon-db-org/reckon-db) event store, accessed over gRPC via the [reckon-gateway](https://codeberg.org/reckon-db-org/reckon-gateway) frontend.

```go
import "codeberg.org/reckon-db-org/reckon-go"

c, err := reckon.Connect(ctx, "beam01.lab:50051")
if err != nil { ... }
defer c.Close()

stores, err := c.Stores().List(ctx)
```

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
| `Causation` | ✅ |
| `Admin` | ✅ |
| `Health` | ✅ |

## `reckon` CLI

A single static binary exposing the whole API as JSON-emitting subcommands
(NDJSON for streams) — for shell/CI use and as the backend for editor
integrations like reckon-nvim. See [`plans/DESIGN_RECKON_CLI.md`](plans/DESIGN_RECKON_CLI.md).

**Install without a Go toolchain** (prebuilt binary from Codeberg releases):

```bash
curl -fsSL https://codeberg.org/reckon-db-org/reckon-go/raw/branch/main/scripts/install.sh | sh
```

Installs `reckon` into `~/.local/bin` (override with `RECKON_BIN_DIR`, pin a
release with `RECKON_VERSION=vX.Y.Z`). Verifies the SHA256 before installing.
(If the repo is ever made private, set `RECKON_TOKEN` to a Codeberg read token —
`install.sh` then sends it as an `Authorization` header for both the script
fetch and the download.)

**With Go:**

```bash
go env -w GOPRIVATE=codeberg.org/reckon-db-org
git config --global url."git@codeberg.org:".insteadOf "https://codeberg.org/"
go install codeberg.org/reckon-db-org/reckon-go/cmd/reckon@latest
```

```bash
reckon --version
reckon -e beam01.lab:50051 -s default_store streams read user-123 --count 10
reckon -e beam01.lab:50051 -s default_store streams watch user-123   # NDJSON
```

Release binaries (`linux/{amd64,arm64}`, `darwin/{amd64,arm64}`,
`windows/amd64`) are built and published to Codeberg releases by
`.github/workflows/release.yml` on every `v*` tag.

## Codegen

Generated gRPC stubs live in `genproto/gatewayv1/`, written by [buf](https://buf.build) from the canonical proto bundle in [reckon-proto](https://codeberg.org/reckon-db-org/reckon-proto):

```bash
cd ../reckon-proto && buf generate --template buf.gen.yaml --output ../reckon-go
```

The generated code is committed (not regenerated on `go build`) so consumers don't need buf installed. Regen on every proto change.

## Versioning

`reckon-go` tracks `reckon-proto` minor versions. SemVer at the Go-API level:

- **MAJOR** — breaking client API change
- **MINOR** — new wrappers, new functionality
- **PATCH** — bug fixes, behaviour unchanged

`reckon-go 0.x.y` against `reckon-proto 0.x.*` is the pinning convention while both are pre-1.0.

## License

Apache-2.0.
