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

`v0.1.0` — the four primary services are wrapped. Five remain on the
generated-stub path (use `c.Conn()` to drive them by hand).

| Service | Wrapper status |
|---|---|
| `Stores` | ✅ |
| `Streams` | ✅ |
| `Subscriptions` | ✅ |
| `Snapshots` | ✅ |
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
