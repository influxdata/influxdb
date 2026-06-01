# `/health` and `/ready`

A reference and operator guide for the two unauthenticated diagnostic
endpoints served by `influxd`.

## Overview

`influxd` serves two diagnostic endpoints at the root of the HTTP server:

- **`/ready`** — readiness gates. Reports whether each one-time startup
  phase (KV migrations, SQL migrations, engine open, shard enumeration,
  service init, etc.) has completed. Use it to decide when a node is
  ready to accept traffic.
- **`/health`** — liveness contributors. Reports whether each subsystem
  is currently healthy. Recomputed on every request. Use it for ongoing
  liveness monitoring after startup has completed.

Both endpoints are mounted by `HealthReadyHandler` at root **before** the
full API handler is installed (`http/check_handler.go`,
`http/api_handler.go`). They are available from the moment the PID file
is written. Until the main API handler is installed, any request to a
path other than `/health` or `/ready` receives `503 Service Unavailable`
with the body:

```json
{"status":"starting"}
```

This pre-API 503 is byte-exact (pinned by
`http/check_handler_jsoncompat_test.go`).

## Endpoint summary

| Path                            | Methods | 200 body status | 503 body status   |
|---------------------------------|---------|-----------------|-------------------|
| `/health`, `/health/`           | GET     | `"pass"`        | `"fail"`          |
| `/ready`, `/ready/`             | GET     | `"ready"`       | `"starting"`      |
| any other path (pre-API)        | any     | —               | `"starting"`      |

Trailing slashes are accepted on both endpoints. Query parameters are
ignored--for example, `/health?cachebust=1` is matched as `/health`.

Path constants are defined in `http/handler.go` as `HealthPath` and
`ReadyPath`.

## Response headers

Every response written by `HealthReadyHandler` carries:

| Header              | Value                              |
|---------------------|------------------------------------|
| `Content-Type`      | `application/json; charset=utf-8`  |
| `X-Influxdb-Build`  | `OSS`                              |
| `X-Influxdb-Version`| The build version string           |

Source: `http/check_handler.go`, `NewHealthReadyHandler`.

---

## `/health`

### JSON envelope

```text
{
  "name":    "influxdb",          // constant
  "status":  "pass" | "fail",     // aggregate over all health checks
  "message": "<top-level message>",
  "checks":  [ <Check>, ... ],    // always present; may be []
  "version": "<build version>",
  "commit":  "<build commit>"
}
```

Each entry in `checks` is:

```text
{
  "name":    "<check name>",      // always present
  "status":  "pass" | "fail",     // always present
  "message": "<detail>"           // omitted when empty
}
```

Per-check `message` and the rarely-used `checks` sub-array are
`omitempty` (see `kit/check/response.go`).

### Status codes

- `200 OK` when **every** registered health check reports `"pass"`.
- `503 Service Unavailable` when **any** check reports `"fail"`.

Aggregation rule: any single `"fail"` makes the aggregate `"fail"`
(`kit/check/check.go`).

### Top-level `message`

- On `200`, `message` is the constant string `"healthy"`.
- On `503`, `message` is taken from the **first** failing check that has
  a non-empty message. If no failing check has a message, the body falls
  back to `"fail"`. If there are no failing checks at all (the response
  was forced to fail by some other path), it falls back to `"starting"`.
  See `firstFailureMessage` in `http/check_handler.go`.

### Example: 200 with one passing check

```json
{
  "name": "influxdb",
  "status": "pass",
  "message": "healthy",
  "checks": [
    {
      "name": "alpha",
      "status": "pass"
    }
  ],
  "version": "<build version>",
  "commit": "<build commit>"
}
```

### Example: 200 with no checks registered

```json
{
  "name": "influxdb",
  "status": "pass",
  "message": "healthy",
  "checks": [],
  "version": "<build version>",
  "commit": "<build commit>"
}
```

### Example: 503 with one failing check

```json
{
  "name": "influxdb",
  "status": "fail",
  "message": "unreachable",
  "checks": [
    {
      "name": "query",
      "status": "fail",
      "message": "unreachable"
    }
  ],
  "version": "<build version>",
  "commit": "<build commit>"
}
```

(JSON trees taken verbatim from `http/check_handler_jsoncompat_test.go`.)

### Registered `/health` checks

The launcher registers the following named health checks. The exact set
depends on whether the server is running with on-disk metadata or
in-memory metadata (test mode).

| Check name        | Disk mode | Memory mode |
|-------------------|:---------:|:-----------:|
| `bolt`            |     ✓     |             |
| `sqlite`          |     ✓     |      ✓      |
| `query`           |     ✓     |      ✓      |
| `influxql`        |     ✓     |      ✓      |
| `task-scheduler`  |     ✓     |      ✓      |
| `shards`          |     ✓     |      ✓      |

Source of truth: `cmd/influxd/launcher/health_ready_test.go`.

#### `bolt`

Background prober that runs a no-op `bolt.View` every
`DefaultProbeInterval` (1 second). The result is wrapped in a
`FreshnessResponse` with a staleness budget of `DefaultProbeStaleness`
(5 seconds). If no fresh probe result is recorded within the budget, the
check flips to `"fail"` with a message of the form:

```text
stale: last probe <age> ago (threshold 5s)
```

Other failure modes:

- `"bolt database not open"` — store was closed or never opened.
- The underlying error string from `bolt.View` when a probe transaction
  returns an error.

Source: `bolt/kv.go`, `kit/check/freshness.go`.

#### `sqlite`

Synchronous `db.PingContext` performed under the SqlStore read lock so a
concurrent `RestoreSqlStore` swap cannot tear the handle. Probe duration
is bounded by `check.DefaultProbeTimeout` (500ms). Failure modes:

- `"sqlite database not open"` — store was closed or never opened.
- The underlying error string from `PingContext` when the probe fails or
  times out.

Source: `sqlite/sqlite.go`.

#### `query`, `influxql`

Liveness contributors registered by the launcher for the Flux query
controller and InfluxQL proxy executor respectively. A `"fail"` on
either indicates the corresponding query path is not currently serving
requests.

Source: `query/bridges.go`, `influxql/query/proxy_executor.go`,
`cmd/influxd/launcher/launcher.go`.

#### `task-scheduler`

Compares the task scheduler's next-run timestamp (`TreeScheduler.When()`)
to wall-clock time. Possible states:

| Condition                                              | Status | Message                          |
|--------------------------------------------------------|--------|----------------------------------|
| `When()` returns zero (no scheduled work)              | pass   | `scheduler idle: no scheduled runs` |
| `When()` is in the future                              | pass   | `next run in <duration>`         |
| `When()` is in the past by ≤ 30s (the pulse threshold) | pass   | `on time, dispatch lag <duration>` |
| `When()` is in the past by > 30s                       | fail   | `scheduler stalled: next run due <duration> ago` |

The threshold is `DefaultSchedulerPulseThreshold = 30 * time.Second`
(`cmd/influxd/run/scheduler_pulse.go`).

#### `shards`

Reports the accumulated shard-load errors observed during engine
startup. `"pass"` until at least one shard fails to load; thereafter
`"fail"` with a message of the form:

```text
<n> shard(s) failed to load: shard <id>: <err>; shard <id>: <err>; ...
```

The same name (`shards`) appears in `/ready` with different semantics —
see below.

Source: `cmd/influxd/run/startup_logger.go`.

---

## `/ready`

### JSON envelope

```text
{
  "status":  "ready" | "starting",   // aggregate over all ready gates
  "started": "<RFC3339Nano timestamp>",
  "up":      "<duration>",
  "checks":  [ <Check>, ... ]        // omitted on 200; failing-only on 503
}
```

Notes:

- `started` is the time `HealthReadyHandler` was constructed. It does
  not change across requests.
- `up` is `time.Since(started)`, formatted as a `toml.Duration` string
  (e.g. `"2.5s"`, `"1h23m45.6s"`).
- `checks` is `omitempty`. On a `200` response it is **absent
  entirely**, not an empty array. On a `503` it contains **only the
  failing** gates.

Source: `http/check_handler.go`.

### Status codes

- `200 OK` with body `"status": "ready"` when **every** registered ready
  gate has been signaled `Ready()`.
- `503 Service Unavailable` with body `"status": "starting"` when **any**
  gate has not yet been signaled (or has been signaled `Unready()` during
  shutdown).

### Example: 200 (no failing checks)

```json
{
  "status": "ready",
  "started": "2026-05-26T15:42:30.123456789Z",
  "up": "1m32.4s"
}
```

`checks` is omitted by `omitempty`.

### Example: 503 (one gate unsignaled)

```json
{
  "status": "starting",
  "started": "2026-05-26T15:42:30.123456789Z",
  "up": "1.2s",
  "checks": [
    {
      "name": "metastores",
      "status": "fail",
      "message": "not ready"
    }
  ]
}
```

Every unsignaled `ReadyGate` emits the default message `"not ready"`
(`kit/check/helpers.go`). The `shards` gate emits different messages.
See below.

### Registered ready gates

The launcher registers eight ready gates in this order:

1. `bolt` — KV (BoltDB) migrations complete.
2. `sqlite` — SQLite migrations complete.
3. `engine` — storage engine `Open` returned successfully.
4. `replications` — replication service `Open` returned successfully.
5. `query` — Flux query controller initialized.
6. `tasks` — task system initialized (pre-signaled when started with
   `--no-tasks`).
7. `task-scheduler` — `TreeScheduler` started.
8. `shards` — shard enumeration and loading complete (see below for
   progress states).

Source of truth: `cmd/influxd/launcher/health_ready_test.go` and
`cmd/influxd/launcher/subsystems.go`.

Each gate is binary. A single `Ready()` call latches it to `"pass"` for
the life of the process. During shutdown, the launcher calls `Unready()`
on every gate it owns, so `/ready` returns `503` while InfluxDB shuts
down those subsystems.

### `shards` ready states (progressive)

The `shards` ready gate is the only one that reports progress before
latching. Its observable states are:

| When                                                                            | Status | Message                                       |
|---------------------------------------------------------------------------------|--------|-----------------------------------------------|
| Engine has not yet enumerated any shard                                         | fail   | `waiting for shard enumeration`               |
| Enumeration started, some shards still loading                                  | fail   | `loading shards N.N% (<completed> / <total>)` |
| `engine.Open` returned with no error                                            | pass   | `ready: <n> shards loaded in <duration>`      |
| `engine.Open` returned an error (terminal)                                      | fail   | `shard loading failed: <error>`               |

The percentage updates every time an individual shard finishes loading;
it is computed as `completed / total * 100` against atomic counters.

Source: `cmd/influxd/run/startup_logger.go`.

---

## Operator how-to

### When the endpoints become available

`/health` and `/ready` start serving as soon as the PID file is written.
This is intentionally earlier than the rest of the API: a probe agent
can begin polling immediately and observe startup progress through the
gate state.

While the API handler is still being assembled, any request to a path
other than `/health` or `/ready` returns `503` with:

```json
{"status":"starting"}
```

This includes `/api/v2/*`, `/query`, `/write`, etc. Once the main API
handler is installed via `SetHandler`, those paths are served normally.

### Picking the right endpoint

Use `/ready`:

- To gate traffic during boot — for example, in a load balancer's
  health check or a Kubernetes startup probe.
- To observe startup progress (the `shards` gate reports a percentage).
- For a clean shutdown signal: when the process begins shutting down,
  the launcher flips gates back to `Unready()` and `/ready` returns
  `503` before the listener stops accepting connections.

Use `/health`:

- For ongoing liveness checks once the process is up.
- To detect transient degradation (a stuck `bolt` probe, a stalled
  scheduler, a shard load failure that occurred after startup
  completed).

> [!IMPORTANT]
> Once a `/ready` gate has been signaled `Ready()`, it does not transition
> back to `"fail"` unless the launcher calls `Unready()` (shutdown). So
> `/ready` will not flap during normal operation. `/health` is recomputed
> on every request and **will** reflect transient failures.

### curl examples

Probe `/ready` and inspect the response--for example:

```sh
curl -sS -o body.json -w '%{http_code}\n' http://localhost:8086/ready
```

The command prints the HTTP status code to stdout and writes the
response body to `body.json`. A ready instance returns `200` and the
following body:

```json
{
  "status": "ready",
  "started": "2026-05-26T15:42:30.123456789Z",
  "up": "1m32.4s"
}
```

Probe `/health` the same way:

```sh
curl -sS -o body.json -w '%{http_code}\n' http://localhost:8086/health
```

A healthy instance returns `200` and a body that lists each check:

```json
{
  "name": "influxdb",
  "status": "pass",
  "message": "healthy",
  "checks": [
    { "name": "bolt", "status": "pass" },
    { "name": "sqlite", "status": "pass" },
    { "name": "query", "status": "pass" },
    { "name": "influxql", "status": "pass" },
    { "name": "task-scheduler", "status": "pass", "message": "scheduler idle: no scheduled runs" },
    { "name": "shards", "status": "pass" }
  ],
  "version": "<build version>",
  "commit": "<build commit>"
}
```

Probe `/ready` during startup to watch the `shards` gate progress--for example:

```sh
curl -sS -o body.json -w '%{http_code}\n' http://localhost:8086/ready
```

The command returns `503`, and the body lists only the checks that
haven't passed:

```json
{
  "status": "starting",
  "started": "2026-05-26T15:42:30.123456789Z",
  "up": "4.2s",
  "checks": [
    { "name": "engine",       "status": "fail", "message": "not ready" },
    { "name": "replications", "status": "fail", "message": "not ready" },
    { "name": "query",        "status": "fail", "message": "not ready" },
    { "name": "tasks",        "status": "fail", "message": "not ready" },
    { "name": "task-scheduler","status": "fail", "message": "not ready" },
    { "name": "shards",       "status": "fail", "message": "loading shards 47.0% (94 / 200)" }
  ]
}
```

Distinguish a `200` from a `503` using the body alone--for example:

```sh
curl -sS http://localhost:8086/ready | jq -r .status
```

```text
ready
```

Do the same for `/health`:

```sh
curl -sS http://localhost:8086/health | jq -r .status
```

```text
pass
```

The HTTP status code is the authoritative signal; the body string is for
human readability.

---

## Troubleshooting: per-subsystem failure scenarios

For each scenario below, the JSON snippet is the relevant portion of the
`/health` or `/ready` response — other fields elided for brevity.

### `/ready` 503 — shards still loading

```json
{
  "status": "starting",
  "checks": [
    { "name": "shards", "status": "fail", "message": "loading shards 47.0% (94 / 200)" }
  ]
}
```

**Meaning:** Normal startup. The engine has enumerated 200 shards and 94
have finished opening. The percentage updates each time another shard
completes.

**Action:** Wait for loading to finish. If the percentage stops climbing,
check `influxd` logs for `Finished loading shard` lines. The shard
currently in flight is logged when it completes.

### `/ready` 503 — shards enumeration not yet started

```json
{
  "status": "starting",
  "checks": [
    { "name": "shards", "status": "fail", "message": "waiting for shard enumeration" }
  ]
}
```

**Meaning:** The engine has not yet begun calling `AddShard`. Either the
engine is still in early initialization (look for the `engine` gate also
failing with `"not ready"`) or the engine is blocked before
enumeration. Check logs for engine open progress.

### `/ready` 503 — terminal shard load failure

```json
{
  "status": "starting",
  "checks": [
    { "name": "shards", "status": "fail", "message": "shard loading failed: open /var/lib/influxdb2/engine/data/2/.../000000001-000000001.tsm: input/output error" }
  ]
}
```

**Meaning:** `engine.Open` returned an error and the `shards` gate has
latched into a terminal failure. Restarting will not clear this without
addressing the underlying error in the message. Check disk health and
file permissions on the data directory.

### `/health` 503 — bolt prober stale

```json
{
  "status": "fail",
  "message": "stale: last probe 12.3s ago (threshold 5s)",
  "checks": [
    { "name": "bolt", "status": "fail", "message": "stale: last probe 12.3s ago (threshold 5s)" }
  ]
}
```

**Meaning:** The background bolt prober has not recorded a fresh result
within the 5-second staleness budget. Either the prober goroutine is
wedged in a `db.View` (the bolt mmap is unresponsive — check disk and
kernel logs) or the host is under such severe scheduling pressure that
the 1-second probe loop cannot run. Look at `iostat`, `dmesg`, and the
`influxd` process's CPU/memory state.

### `/health` 503 — scheduler stalled

```json
{
  "status": "fail",
  "message": "scheduler stalled: next run due 1m45s ago",
  "checks": [
    { "name": "task-scheduler", "status": "fail", "message": "scheduler stalled: next run due 1m45s ago" }
  ]
}
```

**Meaning:** The task scheduler's next-run timestamp is more than 30
seconds behind wall clock. Its dispatch loop fired a timer but never
advanced. If this fires only at boot and clears within a few seconds,
it's likely a cold-start dispatch and not a real wedge.

**Action:** Look for blocked goroutines in the scheduler using the
runtime profile or a stack dump.

### `/health` 503 — sqlite not open

```json
{
  "status": "fail",
  "message": "sqlite database not open",
  "checks": [
    { "name": "sqlite", "status": "fail", "message": "sqlite database not open" }
  ]
}
```

**Meaning:** The SQLite metadata store handle is nil because the store
was closed or never opened. InfluxDB reports this state only briefly,
while it starts up (before SQL migrations complete) or while it shuts
down.

**Action:** If the failure persists, the SQLite store failed to open.
Check `influxd` logs for the underlying error.

### `/health` 503 — sqlite ping error

```json
{
  "status": "fail",
  "message": "context deadline exceeded",
  "checks": [
    { "name": "sqlite", "status": "fail", "message": "context deadline exceeded" }
  ]
}
```

**Meaning:** `db.PingContext` did not return within the 500ms probe
deadline, or returned an I/O error. Check disk health on the SQLite
database file and whether another process is holding a long-running
write transaction.

### `/health` 503 — accumulated shard load failures

```json
{
  "status": "fail",
  "message": "3 shard(s) failed to load: shard 41: I/O error; shard 87: corrupt index; shard 102: I/O error",
  "checks": [
    { "name": "shards", "status": "fail",
      "message": "3 shard(s) failed to load: shard 41: I/O error; shard 87: corrupt index; shard 102: I/O error" }
  ]
}
```

**Meaning:** One or more shards failed to load during engine open.
Errors accumulate — even if the engine itself opened successfully,
`/health` will continue to report this until restart. Each `shard <id>`
in the message identifies a specific shard directory under the
configured data path; address each shard's underlying error before
restarting.

### `/health` 503 — all checks pass but the body says `"fail"`

**Meaning:** `/health` returned `503` but the `checks` array contains
only `"pass"` entries. The response was constructed under an inconsistent
observation, where a check transitioned between the aggregation walk and
the per-check render.

**Action:** Retry the request. The inconsistency window is bounded by the
probe interval. If the condition persists across many requests, file an
issue with the body and headers attached.

---

## Sources

This document is derived from the implementation introduced in commit
`67afeb385b` (PR #27370). Authoritative source files:

- `http/check_handler.go` — endpoint handler, JSON envelopes.
- `http/check_handler_jsoncompat_test.go` — pinned wire format.
- `http/handler.go` — `HealthPath`, `ReadyPath` constants.
- `kit/check/check.go` — status enum, aggregation rule.
- `kit/check/freshness.go` — staleness model and stale-message format.
- `kit/check/helpers.go` — `ReadyGate`, `DefaultProbeTimeout`.
- `kit/check/response.go` — per-check JSON shape and `omitempty` rules.
- `bolt/kv.go` — bolt probe and failure messages.
- `sqlite/sqlite.go` — sqlite probe and failure messages.
- `cmd/influxd/run/scheduler_pulse.go` — task-scheduler check.
- `cmd/influxd/run/startup_logger.go` — shards progress and accumulated
  shard load errors.
- `cmd/influxd/launcher/launcher.go` — gate registration, `Ready` /
  `Unready` call sites.
- `cmd/influxd/launcher/subsystems.go` — canonical subsystem names.
- `cmd/influxd/launcher/health_ready_test.go` — authoritative
  `/health` and `/ready` check sets.
