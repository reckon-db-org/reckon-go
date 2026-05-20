# DESIGN: `reckon` CLI

**Status:** Proposed
**Date:** 2026-05-20
**Owner:** reckon-go
**Location:** `cmd/reckon/` (new), built from the existing `reckon-go` service packages.

---

## 1. Purpose

A single static binary that exposes the full reckon-go API surface (9 services,
62 RPCs) as subcommands emitting JSON to stdout. Primary consumers:

1. **reckon-nvim** — pure-Lua plugin drives the binary via `vim.system` /
   `jobstart`, exchanging JSON (unary) and NDJSON (streams). No gRPC, protobuf,
   or HTTP/2 in Lua.
2. **Shell / CI** — `reckon … | jq`, scripting, smoke tests.
3. **lazyreckon family** — shared muscle for any shell-driven tool.

Design rule: **gRPC stays inside the binary.** Output is plain JSON. The binary
is the only thing that links reckon-go.

---

## 2. Invocation

```
reckon [global-flags] <group> <command> [positional-args] [command-flags]
```

Groups map 1:1 to reckon-go service packages:
`stores · streams · subs · snapshots · schema · temporal · causation · admin · health · catalogue`

### 2.1 Global flags

| Flag | Env | Default | Meaning |
|------|-----|---------|---------|
| `--endpoint`, `-e` | `RECKON_ENDPOINT` | `localhost:50051` | gateway `host:port` |
| `--store`, `-s` | `RECKON_STORE` | — | store id; required by store-bound groups |
| `--timeout` | `RECKON_TIMEOUT` | `5s` | per-RPC deadline (Go duration) |
| `--bytes` | `RECKON_BYTES` | `auto` | how byte fields render: `auto` \| `base64` (see §4.3) |
| `--pretty` | — | `false` | pretty-print unary JSON (default compact) |
| `--version`, `-V` | — | — | print `{client, api_compat}` and exit |
| `-h`, `--help` | — | — | help for any node |

Gateway-wide groups (`health status`, `health server-info`, `catalogue …`,
`stores …`) ignore `--store`. Everything else errors with exit 2 if `--store`
is absent.

### 2.2 Output discipline

- **Unary commands** write exactly one JSON value to **stdout** (object or
  array), then exit. No envelope, no log noise — clean for `jq` and
  `vim.json.decode`.
- **Streaming commands** write **NDJSON**: one JSON object per line, each
  tagged with `type` (§5). stdout is line-buffered and flushed per frame.
- **All diagnostics and errors** go to **stderr** as a single JSON object
  (§6). stdout stays machine-clean on failure (may be empty or a partial
  NDJSON prefix for streams).

---

## 3. Command tree

`(S)` = requires `--store`. `(G)` = gateway-wide, ignores `--store`.
`→` = stdout shape (types defined in §4). `⇄` = NDJSON stream.

### stores `(G)`
```
reckon stores list                         → [Instance]
reckon stores get <store-id>               → [Instance]
reckon stores watch [--types announced,retired]   ⇄ StoreEvent
```

### streams `(S)`
```
reckon streams list                                          → [string]
reckon streams read <stream> [--from N] [--count M] [--backward]   → [Event]
reckon streams watch <stream> [--from N] [--count M]               ⇄ Event
reckon streams version <stream>                              → {"version": int64}
reckon streams delete <stream>                               → {"deleted": true}
reckon streams append <stream> [--expect SPEC]               → AppendResult     (events on stdin)
reckon streams by-types <t1,t2,…> [--batch N]                → [Event]
reckon streams by-tags  <t1,t2,…> [--match any|all] [--batch N]    → [Event]
reckon streams all [--offset N] [--limit M]                  → [Event]
```
`--expect SPEC` ∈ `no-stream` (-1) | `any` (-2) | `exists` (-4) | `<N>` (exact).
Default `any`. `append` reads ProposedEvents from stdin (§4.7).

### subs `(S)`
```
reckon subs list                                  → [SubInfo]
reckon subs get <name>                            → SubInfo
reckon subs lag <name>                            → Lag
reckon subs create <name> --type T --selector SEL [--from N] [--pool K]   → {"subscription_id": string}
reckon subs remove <name> --type T --selector SEL → {"removed": true}
reckon subs ack <stream> <name> <version>         → {"acked": true}
reckon subs consume <name> --type T --selector SEL [--from N] [--pool K] [--ack-mode auto|none|stdin]   ⇄ Delivery
```
`--type T` ∈ `stream | event_type | event_pattern | event_payload | tags`.
`--ack-mode` (§5.2) default `auto`; nvim uses `stdin`.

### snapshots `(S)`
```
reckon snapshots list <source-uuid> <stream-uuid>          → [Snapshot]
reckon snapshots list-all                                  → [Snapshot]
reckon snapshots at <source-uuid> <stream-uuid> <version>  → Snapshot
reckon snapshots latest <source-uuid> <stream-uuid>        → Snapshot
reckon snapshots save <source-uuid> <stream-uuid> <version>    → {"saved": true}   (data on stdin)
reckon snapshots delete <source-uuid> <stream-uuid> <version>  → {"deleted": true}
```

### schema `(S)`
```
reckon schema list                       → [SchemaDef]
reckon schema get <event-type>           → SchemaDef
reckon schema version <event-type>       → {"version": uint32}
reckon schema register <event-type>      → {"registered": true}   (schema blob on stdin)
reckon schema unregister <event-type>    → {"unregistered": true}
reckon schema upcast                     → [Event]                (events on stdin)
```

### temporal `(S)`
```
reckon temporal until <stream> <ts> [--batch N]          → [Event]
reckon temporal range <stream> <from-ts> <to-ts> [--batch N]   → [Event]
reckon temporal version-at <stream> <ts>                 → {"version": int64}
```
`<ts>` accepts RFC3339 (`2026-05-20T14:00:00Z`) or epoch-ms (`1747749600000`).

### causation `(S)`
```
reckon causation effects <event-id>          → [Event]
reckon causation cause <event-id>            → Event
reckon causation chain <event-id>            → [Event]
reckon causation correlated <correlation-id> → [Event]
reckon causation graph <event-id>            → GraphNode   (recursive, §4.6)
```

### admin `(S)`
```
reckon admin stats                                    → StoreStats
reckon admin stream-info <stream>                     → StreamInfo
reckon admin event-types                              → [EventTypeCount]
reckon admin scavenge <stream> [--dry-run] [--opt k=v …]   → ScavengeResult
reckon admin scavenge-matching <pattern> [--opt k=v …]     → [ScavengeResult]
reckon admin links list                               → [LinkSpec]
reckon admin links get <name>                         → LinkSpec
reckon admin links create <name> --source S --target T [--opt k=v …]   → {"created": true}
reckon admin links delete <name>                      → {"deleted": true}
reckon admin links start <name>                       → {"started": true}
reckon admin links stop <name>                        → {"stopped": true}
reckon admin links info <name>                        → LinkRuntime
```

### health
```
reckon health check                  (S) → CheckResult
reckon health status                 (G) → HealthResult
reckon health cluster-consistency    (S) → ClusterResult
reckon health membership-consensus   (S) → ClusterResult
reckon health raft-log               (S) → ClusterResult
reckon health memory-level           (S) → {"level": "normal|high|critical"}
reckon health memory-stats           (S) → MemoryStats
reckon health server-info            (S) → ServerInfo
```
Only `health status` is gateway-wide. In catalogue mode the gateway routes
the other health RPCs to the BEAM owning the store, so they require `--store`
(an empty store_id returns InvalidArgument). Verified against the fleet
(gateway 0.5.4): server-info reported `reckon_gateway_version "0.5.4"`.

### catalogue `(G)`
```
reckon catalogue status   → CatalogueStatus
reckon catalogue reload   → CatalogueReloadResult
```

---

## 4. JSON schemas (unary)

Field names are `snake_case`. Types map from reckon-go structs verbatim.

### 4.1 Conventions

- **Timestamps** → RFC3339 nanosecond UTC string, e.g. `"2026-05-20T14:03:11.123456789Z"`.
  Zero `time.Time` → `null`.
- **uint64 / int64** → JSON number. (Values are well under 2^53 in practice;
  if a field can exceed that it is documented as a string. None currently do.)
- **`map[string]string` / `map[string]uint64`** → JSON object; empty → `{}`.
- **Byte fields** → see §4.3.
- **Duration** → integer milliseconds (e.g. `Instance.timeout` → `"timeout_ms"`).

### 4.2 `Event` (the `RecordedEvent` — used everywhere)

```json
{
  "event_id": "0190f3a2-…",
  "event_type": "user_registered_v1",
  "stream_id": "user-123",
  "version": 7,
  "data": { "name": "Ada" },
  "metadata": { "actor": "system" },
  "data_b64": "eyJuYW1lIjoiQWRhIn0=",
  "metadata_b64": "eyJhY3RvciI6InN5c3RlbSJ9",
  "tags": ["user", "vip"],
  "timestamp": "2026-05-20T14:03:11.123Z",
  "epoch": "2026-05-20T14:03:11.000Z",
  "data_content_type": "application/json",
  "metadata_content_type": "application/json",
  "prev_event_hash_b64": "9f2c…"
}
```

`prev_event_hash_b64` omitted (or `null`) when empty. `data`/`metadata`
presence depends on `--bytes` (§4.3).

### 4.3 Byte-field rendering (`--bytes`)

Byte fields appear on: `Event.data/metadata`, `Snapshot.data/metadata`,
`SchemaDef.schema`, all `*_hash` fields.

- `--bytes auto` (**default**, what nvim uses): for each byte field `X`:
  - always emit `X_b64` (lossless base64) **for hashes**; for data/metadata emit
    `X_b64` too so the consumer can always recover raw bytes.
  - additionally emit a decoded `X`:
    - `content_type == application/json` and parses → native JSON value
    - else valid UTF-8 → JSON string
    - else → field omitted (only `X_b64` present)
- `--bytes base64`: emit only `X_b64`; no decoded convenience field. Lossless,
  stable, best for piping/round-tripping.

Rationale: nvim renders `data` directly when present, falls back to `data_b64`;
scripts that round-trip use `base64`.

### 4.4 `Instance` (stores)

```json
{ "store_id": "default_store", "node": "reckon@beam01", "mode": "cluster",
  "data_dir": "/bulk0/reckon/default_store", "timeout_ms": 5000,
  "registered_at": "2026-05-20T13:00:00Z" }
```
`mode` ∈ `unspecified | single | cluster`.

### 4.5 Subscriptions / snapshots / schema / admin / health

```jsonc
// SubInfo
{ "id": "sub-…", "name": "projector", "type": "event_type",
  "selector": "user_registered_v1", "created_at": "…", "pool_size": 4, "checkpoint": 42 }
// Lag
{ "lag": 3, "current_checkpoint": 42, "latest_version": 45 }
// Snapshot
{ "stream_id": "user-123", "version": 7, "data": {…}, "data_b64": "…",
  "metadata": {…}, "metadata_b64": "…", "timestamp": "…", "anchor_hash_b64": "…" }
// SchemaDef
{ "event_type": "user_registered_v1", "version": 2, "schema": {…}, "schema_b64": "…" }
// StoreStats
{ "total_streams": 12, "total_events": 3400, "total_subscriptions": 5,
  "total_snapshots": 8, "details": {} }
// StreamInfo
{ "stream_id": "user-123", "version": 7, "event_count": 8,
  "created_at": "…", "last_event_at": "…", "event_types": ["user_registered_v1","user_promoted_v1"] }
// EventTypeCount
{ "event_type": "user_registered_v1", "count": 120 }
// ScavengeResult
{ "events_removed": 10, "events_remaining": 90, "space_reclaimed_bytes": 4096, "details": {} }
// LinkSpec / LinkRuntime
{ "name": "by-cat", "source": "$all", "target": "cat-stream", "options": {} }
{ "name": "by-cat", "status": "running", "events_processed": 1234, "details": {} }
// CheckResult / ClusterResult
{ "status": "healthy", "details": {} }
{ "status": "healthy", "details": {} }   // status ∈ healthy|degraded|unhealthy
// HealthResult
{ "status": "healthy", "stores": {"default_store": 4}, "total_workers": 4,
  "node": "reckon@beam01", "timestamp": "…" }
// MemoryStats
{ "used_bytes": 123, "total_bytes": 456, "usage_percent": 27.0, "breakdown": {"ets": 100} }
// ServerInfo
{ "reckon_db_version": "2.3.2", "reckon_gateway_version": "0.5.4",
  "api_compatibility_version": "0.3", "integrity_enabled": true,
  "integrity_algo": "hmac-sha256", "hmac_key_id": 1 }
// AppendResult
{ "version": 8, "position": 3401, "count": 1 }
```

### 4.6 `GraphNode` (causation graph, recursive)

```json
{ "event": { /* Event */ }, "children": [ { "event": {…}, "children": [] } ] }
```

### 4.7 `ProposedEvent` (stdin input for `streams append`, `schema upcast`)

Accepts a JSON array **or** NDJSON (one object per line). Per object:

```json
{ "event_id": "optional-uuid", "event_type": "user_registered_v1",
  "data": {…}            // OR "data_b64": "…"
  "metadata": {…},       // OR "metadata_b64": "…"  (optional)
  "tags": ["user"],
  "data_content_type": "application/json",          // default application/json
  "metadata_content_type": "application/json" }
```
`data` (native JSON) is marshaled to bytes with `data_content_type`; `data_b64`
takes precedence if both present. Missing `event_id` → CLI generates a UUIDv7.

---

## 5. Streaming protocol (NDJSON)

Applies to `stores watch`, `streams watch`, `subs consume`. Each line is one
frame object with a `type` discriminator. First frame is always `ready`.

```jsonc
{"type":"ready","at":"2026-05-20T14:03:00Z"}              // stream established
{"type":"event","event":{ /* Event */ }}                  // stores/streams watch
{"type":"store_event","change":"announced","instance":{…},"at":"…"}   // stores watch
{"type":"delivery","event":{ /* Event */ },"checkpoint":42}           // subs consume
{"type":"error","code":"unavailable","message":"…"}       // terminal; process exits nonzero
{"type":"end","reason":"eof|count_reached|client_eof"}     // terminal; clean, exit 0
```

- Exactly one terminal frame (`end` or `error`) closes every stream.
- On SIGINT/SIGTERM (or stdin EOF, see §5.2) the CLI cancels the gRPC stream,
  emits `{"type":"end","reason":"client_eof"}`, exits 0.
- `stores watch` uses `store_event`; `streams watch` uses `event`; `subs
  consume` uses `delivery`.

### 5.1 Why `ready`

nvim needs a definite "subscription is live" signal before it shows the view as
connected (vs. still dialing). The `ready` frame provides it without a timeout
guess.

### 5.2 Ack modes (`subs consume`)

| `--ack-mode` | Behavior |
|--------------|----------|
| `auto` (default) | CLI calls `Ack` immediately after emitting each `delivery`. At-most-once for the consumer; simplest for ad-hoc tailing. |
| `none` | Never acks. Pure observation; checkpoint unchanged server-side. |
| `stdin` | **nvim mode.** CLI reads ack commands from its stdin, one JSON per line: `{"ack": 42}`. Lets the plugin ack only after it has durably handled the event. Closing stdin = graceful shutdown (→ `end`/`client_eof`). |

`stdin` mode is the robust path for a UI: emit `delivery` → plugin processes →
plugin writes `{"ack":<checkpoint>}` → CLI acks. Backpressure is natural (the
plugin reads deliveries as fast as it wants).

---

## 6. Errors (stderr)

Single JSON object to stderr on any failure:

```json
{ "error": { "code": "not_found",
             "message": "store \"ghost\" not in catalogue",
             "grpc_status": 5, "details": {} } }
```

`code` is a stable lowercase string mapped from the gRPC status. For streams,
a partial NDJSON prefix may already be on stdout; the terminal `error` frame
(§5) is also emitted on stdout, and the same object goes to stderr.

### 6.1 gRPC status → `code` → exit code

| gRPC status | `code` | Exit |
|-------------|--------|------|
| OK (0) | — | `0` |
| (usage / bad flags, pre-RPC) | `usage` | `2` |
| NotFound (5) | `not_found` | `3` |
| Unavailable (14) | `unavailable` | `4` |
| DeadlineExceeded (4) | `timeout` | `5` |
| connection refused / no route (dial fail) | `unreachable` | `6` |
| InvalidArgument (3) | `invalid_argument` | `7` |
| FailedPrecondition (9) — e.g. version conflict | `precondition` | `8` |
| AlreadyExists (6) | `already_exists` | `9` |
| anything else | `internal` | `1` |

Optimistic-concurrency conflicts on `append` surface as
`precondition` / exit 8 — distinct so nvim can show "stream moved, reload."

---

## 7. Placement & distribution

**Decision: in-repo, no separate `reckon-cli` repo, no separate Go module.**
The CLI is a thin shell over reckon-go — every leaf is `args → reckon-go call →
JSON`. Its only real dependency is reckon-go itself, so the two move atomically
and share one version. Lives as `cmd/reckon`, alongside the existing
`cmd/probe`, `cmd/filter-probe`, `cmd/validator-probe`.

Dependency isolation (the usual argument for splitting) is handled by using the
**standard library `flag` package only** — no cobra, no third-party CLI deps —
so nothing leaks into reckon-go's `go.mod` or its consumers (lazyreckon, apps
importing `reckon-go/streams` etc.). A separate repo or nested submodule is
therefore unnecessary.

- `go build ./cmd/reckon`.
- CI cross-compiles `linux/{amd64,arm64}`, `darwin/{amd64,arm64}`,
  `windows/amd64`; published as Codeberg release assets named
  `reckon_<version>_<os>_<arch>(.exe)` with SHA256SUMS.
- `reckon --version` prints `{"client":"0.3.0","api_compat":"0.3"}` for the
  plugin to verify compatibility against `health server-info`.
- reckon-nvim resolves the binary in order: `opts.bin` → `$PATH` →
  mason-managed → auto-download matching release asset. mason package name:
  `reckon`.

---

## 8. Implementation notes

- One `*reckon.Client` per process (`reckon.Connect`), reused across the single
  command. `Close()` on exit.
- **Stdlib `flag` only** — no third-party CLI deps (see §7). The command tree is
  hand-routed: `os.Args[1]`/`[2]` select group+command, each leaf owns a
  `flag.FlagSet` for its command-flags; global flags parsed first. A small
  `route` table maps `group command` → handler. No business logic in the CLI —
  every leaf is `args → reckon-go call → JSON encode`.
- Encoding lives in one `encode` package: `Event` → map, byte-field policy,
  timestamp formatting. Single source of truth; unit-tested against golden JSON.
- Streaming leaves share a `pump(ctx, eventsCh, errCh, frameFn)` helper that
  writes `ready`, ranges the channel emitting frames, then writes the terminal
  frame — mirrors reckon-go's `(<-chan T, <-chan error)` idiom exactly.
- Signal handling: `signal.NotifyContext` for SIGINT/SIGTERM cancels the RPC
  context → channels close → terminal frame → exit.

---

## 9. Open questions

1. **Append from nvim** — likely rare; keep stdin-only (no per-event flags) to
   stay simple. Confirm before building richer flag forms.
2. **`--bytes auto` ambiguity** — a field that is JSON object in one event and
   base64 string in another (mixed content types in a stream) is awkward for
   typed consumers. nvim is fine (dynamic), but document loudly. Alternative:
   always-tagged `{ "enc": "...", "val": ... }` — rejected for verbosity.
3. **Auth/TLS** — none today (gateway is LAN-trust). When the gateway gains it,
   add `--tls`, `--ca`, `--token` global flags; out of scope now.
```
