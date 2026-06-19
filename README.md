# reckon-go
[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-support-yellow.svg)](https://buymeacoffee.com/rlefever)

Idiomatic Go client for the [ReckonDB](https://codeberg.org/reckon-db-org/reckon-db) event store, accessed over gRPC via the [reckon-gateway](https://codeberg.org/reckon-db-org/reckon-gateway) frontend.

```go
import "codeberg.org/reckon-db-org/reckon-go"

c, err := reckon.Connect(ctx, "gateway.example.org:50051") // TLS, system roots (the default)
if err != nil { ... }
defer c.Close()

stores, err := c.Stores().List(ctx)
```

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

**With Go** (the repo is public, so this is all you need):

```bash
go install codeberg.org/reckon-db-org/reckon-go/cmd/reckon@latest
```

<details>
<summary>If the repo is ever made private</summary>

```bash
go env -w GOPRIVATE=codeberg.org/reckon-db-org
git config --global url."git@codeberg.org:".insteadOf "https://codeberg.org/"
go install codeberg.org/reckon-db-org/reckon-go/cmd/reckon@latest
```
</details>

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
