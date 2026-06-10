// Package reckon is the idiomatic Go client for the ReckonDB event store,
// accessed over gRPC via the reckon-gateway frontend.
//
// The package name is `reckon`, not `reckongo` — the module path
// (codeberg.org/reckon-db-org/reckon-go) ends in `reckon-go` per the
// org's repo-naming convention, but Go consumers import it as:
//
//	import "codeberg.org/reckon-db-org/reckon-go"
//
//	c, err := reckon.Connect(ctx, "gateway.example.org:50051")
//
// Connect defaults to TLS against the system root pool. Plaintext lab
// gateways need the explicit opt-in:
//
//	c, err := reckon.Connect(ctx, "beam01.lab:50051", reckon.Insecure())
//
// # Top-level facade
//
// `reckon.Client` is the entry point. Per-service operations are
// accessed through sub-clients:
//
//	c.Stores()        // cluster topology discovery
//	c.Streams()       // append + read events
//	c.Subscriptions() // live + persistent subscriptions
//	c.Snapshots()     // per-stream snapshots
//	c.Schema()        // schema registration + upcasting
//	c.Temporal()      // time-travel reads
//	c.Admin()         // scavenge, links, store inspection
//	c.Health()        // standard gRPC health
//
// # Streaming semantics
//
// Server-streaming RPCs (Subscribe, WatchStores, ...) return a Go
// channel of events plus an error channel. Consumers `range` over the
// event channel; cancellation flows through the parent ctx.
//
// This is package documentation only — see CHANGELOG.md and README.md
// for status and roadmap.
package reckon
