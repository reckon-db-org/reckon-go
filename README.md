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

`v0.1.0` — scaffold. The top-level `Client` + transport are wired; per-service sub-clients are stubs and land incrementally.

| Service | Wrapper status |
|---|---|
| `Stores` | 🚧 — first to land (validates the wrapper pattern) |
| `Streams` | ⬜ |
| `Subscriptions` | ⬜ |
| `Snapshots` | ⬜ |
| `Schema` | ⬜ |
| `Temporal` | ⬜ |
| `Causation` | ⬜ |
| `Admin` | ⬜ |
| `Health` | ⬜ |

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
