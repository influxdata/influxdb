# `/health` and `/ready`

A reference and operator guide for the two diagnostic endpoints served by
`influxd`. Both are unauthenticated by default; `--health-auth-enabled`
gates their diagnostic detail behind operator permissions without
breaking credential-free probes. See [Authentication](#authentication).

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

| Path                            | Methods | 200 body status | 503 body status   | Auth gated |
|---------------------------------|---------|-----------------|-------------------|------------|
| `/health`, `/health/`           | GET     | `"pass"`        | `"fail"`          | detail only |
| `/ready`, `/ready/`             | GET     | `"ready"`       | `"starting"`      | detail only |
| any other path (pre-API)        | any     | —               | `"starting"`      | no         |

"Detail only" means the status code and the top-level status are always
served; see [Authentication](#authentication).

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

## Authentication

By default both endpoints are fully unauthenticated: any caller receives
the complete body documented below. This is the historical behavior and
it is unchanged unless you opt in.

`--health-auth-enabled` (also implied by `--hardening-enabled`) requires
**operator permissions** to read the diagnostic detail. This exists
because check messages carry raw error text — filesystem paths,
permission errors, shard ids — which is not appropriate to publish to an
unauthenticated caller in a hardened deployment.

This applies with particular force to the [startup failure
checks](#startup-failure-checks): the message of a failed init phase is
the error that phase produced verbatim, which can name a bolt or sqlite
path, a vault address, or a migration that failed. **With health auth
left at its default (off), those messages are readable by any
unauthenticated caller that can reach the port.** They are not truncated
or sanitized — a startup failure an operator cannot read the cause of is
the problem these checks exist to fix.

### Opting out under `--hardening-enabled`

`--hardening-enabled` turns on every hardening feature, this one
included. If your monitoring parses the `/health` body, **set
`--health-auth-enabled=false` explicitly** and it wins:

```bash
influxd --hardening-enabled --health-auth-enabled=false
```

The same works from `INFLUXD_HEALTH_AUTH_ENABLED=false` or from
`health-auth-enabled: false` in the config file. What matters is that the
option is named somewhere, not where — an option left at its default is
what `--hardening-enabled` is allowed to imply.

Because "named anywhere" counts, `influxd print-config` resolves the
implication before printing, so a hardened server prints
`health-auth-enabled: true`. That matters for the documented workflow of
redirecting `print-config` into a config file: every key it prints
becomes a key you have named, so printing the *unresolved* default would
turn the next start into a silent opt-out. Whichever way you resolved
it, the printed config is a fixed point — start from it and you get the
same server back.

When hardening is on and health auth is off, `influxd` logs a warning at
startup naming what is ungated. Seeing it means the opt-out is in effect;
if you did not intend one, check your config file for a
`health-auth-enabled` key left over from a `print-config` dump.

This escape hatch exists because dropping `--hardening-enabled` is not a
substitute: the flux/pkger IP validator it enables has no per-feature
flag, so there would be no way to keep it while declining this. The
opt-out is honored by `applyHardeningImplications` in
`cmd/influxd/launcher/cmd.go`, and `/api/v2/config` reports the resolved
value — `health-auth-enabled: false` alongside `hardening-enabled: true`
— so what the API reports is always what the server enforces.

`--template-file-urls-disabled`, the other feature `--hardening-enabled`
implies, has no such opt-out. It changes no response body, so nothing
downstream can break on it unannounced.

### What changes when it is enabled

The **HTTP status code never changes**. A credential-free liveness or
readiness probe continues to work exactly as before; only the body is
reduced. What is withheld is every check `message`, the build fields,
and — once something is actually failing — which check it was:

| Endpoint / state    | Authorized                                    | Rejected                              | Unidentifiable †                      |
|---------------------|-----------------------------------------------|---------------------------------------|---------------------------------------|
| `/health` passing   | `name`, `status`, `message`, `checks`, `version`, `commit` | `name`, `status`, `message`, `checks` (no per-check messages) | same as "rejected" |
| `/health` failing   | `name`, `status`, `message`, `checks`, `version`, `commit` | `name`, `status`         | `name`, `status`, `checks` (no messages) |
| `/ready` ready      | `status`, `started`, `up`                     | `status`, `started`, `up`             | `status`, `started`, `up`             |
| `/ready` starting   | `status`, `started`, `up`, `checks`           | `status`, `started`, `up`             | `status`, `started`, `up`, `checks` (no messages) |

† **Rejected** means a credential was resolved and found wanting — or
none was presented, which is every credential-free probe.
**Unidentifiable** means the handler could not ask at all: the
[startup window](#the-startup-window) or a
[wedged KV store](#behavior-when-the-kv-store-is-wedged). Both are global
server state that no caller can bring about, so not being able to ask
releases the check names and statuses — never the messages.

A [saturated resolution cap](#behavior-when-the-kv-store-is-wedged) is
deliberately *not* one of them, and is answered as **rejected**: it is
the one "could not ask" condition a caller can manufacture, by sending
enough concurrent credential-bearing requests, and releasing more for it
would let a flood grant itself the attribution while demoting the
operator probe it crowds out.

**A passing `/health` keeps its documented shape for everyone.** The
aggregate is `pass`, so the `checks` array is just the list of
registered subsystems with `"status":"pass"` — the same list on every
install of the same configuration, saying nothing the `200` does not
already say — and `message` is the constant `"healthy"`. Withholding
those would break every consumer that reads them, on the path that is
true almost all of the time, and protect nothing. What a non-operator
does not get is the per-check messages (the `task-scheduler` check
reports its next-run timing in one) and `version`/`commit`.

Once a check fails, *which* check failed is this server's state rather
than its shape, so it is withheld — along with the top-level `message`,
which is that check's raw error text verbatim:

```json
{"name":"influxdb","status":"fail"}
```

That body is a `check.BasicResponse`, the same type the in-repo
remote-health client decodes, so existing consumers keep parsing it.

`/ready` only ever emits `checks` when it is failing, so on a ready
instance the authorized and unauthorized bodies are identical. `started`
and `up` are not withheld: neither is sensitive, and a probe reading
uptime should keep working.

### Who counts as authorized

The bar is `influxdb.OperPermissions()` — the same one `/api/v2/config`
and `/api/v2/backup` use. Two credentials satisfy it:

- An **operator token**, such as the one minted during onboarding.
- A **session for the initial setup user**, which holds read and write on
  the `instance` resource type. That type is an action-wide wildcard in
  `Permission.matchesV1`.

An **organization owner is not sufficient.** `OwnerPermissions` grants
permissions scoped to an org ID, and an org-scoped permission cannot
satisfy the org-wide (nil `OrgID`) permission `OperPermissions`
requires. On a multi-user install, non-setup users see the reduced body
in the UI. Only the `instance` resource type crosses that line, and it
cannot be attached to an API token — `authorization/middleware_auth.go`
rejects creating one.

### The startup window

The handler begins serving before an authorization service exists, so
early in startup no credential can be validated **at all** — not even an
operator token. This window is a floor, not a wiring oversight:
resolving a token reads the authorization store, and that store cannot
be opened until the KV migrations have finished (its setup rejects a
pre-migration bolt file outright, with `missing required index, upgrade
required`).

Withholding everything for that window would blind an operator during
the one phase — migrating, or hung mid-migration — when these endpoints
are all they have. So while no resolver is installed, both endpoints
release **check names and statuses with the messages stripped**:

```json
{"name":"influxdb","status":"fail","checks":[{"name":"kv","status":"fail"}]}
```

The messages are what stay behind, because that is where startup error
text lives — filesystem paths, addresses, DSNs. `version` and `commit`
stay behind with them. This is a deliberate relaxation, and it applies
to every caller including an anonymous one: during this window there is
nobody to ask.

Only the failing bodies change here. A **passing** `/health` serves the
same thing during the window as it does afterwards, because a passing
`/health` withholds no attribution from anyone.

The same body is served whenever the handler cannot identify a caller,
not only during startup — see
[Behavior when the KV store is wedged](#behavior-when-the-kv-store-is-wedged).

It ends for good the moment a resolver is installed, which the launcher
does immediately after the KV migrations, before the SQL migrations and
before `engine.Open`. From then on the caller's permissions decide, and
a caller who cannot prove operator permissions gets **less** than the
window gave away. A **token-only** resolver goes in first, so an
operator with a token can watch shard-loading progress on `/ready`
throughout the slow part of startup; session (cookie) credentials only
work once the full resolver replaces it, late in startup.

Both edges are logged, so an operator seeing a message-less body can
tell which phase they are in:

```text
Check detail on /health and /ready requires operator permissions; until the
  authorization store opens, both report check names and statuses without messages
Check detail on /health and /ready now gated on operator permissions  {"credentials": "token"}
```

Source: `Launcher.run`, which pins the resolver install directly after
`openMetaStores` — the SQL migrations were split into `migrateSQLStore`
specifically so they land on the far side of it.

### Behavior when the KV store is wedged

Resolving a credential reads bolt, and a bolt `View` cannot be
cancelled. To keep a wedged store from hanging the probe — the exact
failure the background prober exists to survive — credential resolution
is **skipped entirely while the KV health check is failing**.

Nobody can be identified while that holds, operator tokens included. So
this is treated the same way as the [startup window](#the-startup-window)
and for the same reason: **check names and statuses go out, messages do
not**. An operator watching a KV incident still learns which subsystems
are failing, including subsystems that have nothing to do with KV. The
messages — the raw error text this flag exists to gate — need a
credential, and getting one is what the guard is refusing to do. Read
the `influxd` log for those.

If you present an operator token and get names and statuses but no
messages, that is this guard or the startup window, not a credential
problem. A bare `{"name","status"}` on a failing `/health` means your
credential was resolved and rejected — or that the resolution cap below
was saturated when your request arrived.

**Concurrent resolutions are capped** (`maxInflightResolutions`, 8).
The guard above is up to `DefaultProbeStaleness` late, so a store that
wedges just after its last successful probe still looks healthy for
seconds, and any resolution begun in that window never returns.
Uncapped, each one strands a goroutine and a bolt read transaction for
the life of the process. Capped, the damage stops at 8 and every later
request is answered from check state that reads no store at all.

**The resolution rate is capped too** (`maxResolutionsPerSecond`, 2, with
a burst of 16). The concurrency cap above is the wrong quantity for this:
eight slots turning over at bolt speed is thousands of reads a second.
Resolving a token opens a bolt `View` whether or not the token is any
good, so without a rate cap anyone who can reach the port can drive
unbounded reads against the metadata store — through the one endpoint
operators routinely exempt from the rate limiting that fronts everything
else. The budget is per server, not per caller: the resource being
protected is the store, and per-IP state is unbounded memory an anonymous
caller controls.

Credential-free requests spend neither a slot nor budget: the scheme
probe runs first and needs no store, so a fleet of plain liveness probes
cannot crowd out the credentialed operator probe these caps exist to
protect. A credentialed request turned away by either cap is answered as
**rejected** — `{"name","status"}`, no `checks` — never with a different
status code. That costs an operator the attribution for as long as the
condition lasts: for a genuinely wedged store, bounded by
`DefaultProbeStaleness`, after which the guard above takes over and
restores names and statuses for everyone; for a flood, as long as the
flood runs. `influxd` logs a throttled warning while the budget is
exhausted, which is the only way to tell a starved probe from a rejected
credential — they produce the same body.

### Check detail during the window

`--startup-error-linger` and `--health-auth-enabled` interact badly, and
the interaction cannot be designed away. During the
[linger window](#keeping-the-endpoints-alive-after-a-failed-startup) the
store credentials are resolved against has been closed — releasing it is
the point of closing everything but the listener before the wait — so no
caller can be identified for the whole window. By the rule above, being
unable to *ask* releases shape and never content: **every caller,
including one presenting a valid operator token, sees check names and
statuses with the messages stripped.**

What an operator still gets is more than nothing. The `503` is correct,
every check is named with its status, and exactly one `/health` entry is
failing — the subsystems that came up read `pass`, and `shards` reads
`pass` on an engine failure — so *which* phase failed is unambiguous.
Only the reason string is withheld, and the log already carries it at
`ERROR` with a `subsystem` field.

There are three honest mitigations and no fourth:

- Read the log for the reason; the endpoint tells you which subsystem.
- The answer is at least consistent: the auth dependency checker is
  retired at the moment of the freeze, so the body has the same shape
  from the first request of the window to the last.
- Run with health auth off if the message must be readable over HTTP —
  accepting that it is then readable by anyone who can reach the port.

Keeping the store open across the window would restore the detail, but it
would continue holding the flock that the split teardown exists to
release. The PID file remains held independently until final shutdown.

Note that this is the existing policy applied consistently, not a new
hole: a startup failure *before* the authorization store opens already
reached the same reduced body, window or no window.

### Cost

A caller presenting no credential costs nothing extra: the scheme probe
fails before any store access. A valid token costs two bolt reads per
request; an invalid one costs a single read, since the user lookup is
only reached after the token resolves.
A **session cookie is considerably more expensive** — session lookup
recomputes the permission set on every request, which includes a full
scan of the authorization bucket. Prefer tokens for automated polling.

Session renewal is always disabled on this path, so polling `/health`
with a browser session does not extend that session's lifetime.

**Probes do not appear as user activity.** Identifying the caller behind
a probe requires looking its user up, but the resolver holds the user
and session services without the metrics and logging middleware the rest
of the server uses. A credentialed probe therefore does not move
`service_user_new_call_total` or `service_session_call_total` and writes
no service log line. This is deliberate: probe traffic is constant, so
recorded alongside real traffic it does not read as noise on a dashboard
— it reads as a user, forever. The bolt reads above are still real; they
are simply not attributed to anyone.

### Limitations

Suppressing `version` and `commit` from the body does not conceal the
version: `X-Influxdb-Version` is still stamped on every response,
including reduced ones. That header is shared with the root handler and
the whole API, so changing it is out of scope for this flag.

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

> **This envelope is the unauthenticated default.** With
> `--health-auth-enabled` (or `--hardening-enabled`) a caller who cannot
> prove operator permissions still gets `name`, `status`, `message` and
> `checks` on a `200`, but with the per-check messages and the build
> fields removed; on a `503` it gets `{"name","status"}` and no `checks`
> key at all. If you parse `/health`, read
> [Authentication](#authentication) before assuming a field is present.

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

Every check above is registered *after* its subsystem is up, so a server
that failed to reach that point has no entry for it. What it has instead
is the failure check below.

#### Startup failure checks

When an initialization phase in `Launcher.run` fails, the launcher
registers one additional health check named for that phase, whose
message is `"<what was being done>: <error>"`. It is the only entry that
subsystem will have — the normal check for it is never reached — so a
name appears at most once in `checks[]`.

The phases that can appear, beyond the subsystem names already listed:

| Check name            | Failing phase                                  |
|-----------------------|------------------------------------------------|
| `feature-flags`       | `--feature-flags` overrides could not be parsed |
| `pidfile`             | PID file could not be written                  |
| `http-server`         | listener could not be bound                    |
| `meta-store`          | KV migrations, or an unknown `--store` value   |
| `authorization`       | authorization store could not be opened        |
| `authorization-v1`    | v1 authorization store could not be opened     |
| `secrets`             | secret store, vault service, or unknown `--secret-store` |
| `meta-client`         | meta client could not be opened                |
| `engine`              | prior-version check, or `engine.Open`          |
| `replications`        | replication service could not be opened        |
| `notification-rules`  | notification rule store could not be created   |
| `scraper`             | scraper scheduler could not be created         |
| `labels`              | label store could not be created               |
| `api`                 | config handler could not be created            |

`feature-flags` and `pidfile` run before the HTTP listener exists, and
`http-server` *is* the listener failing to bind, so none of those three
can be served to a probe: their failures reach the log only. They are
listed for completeness.

Every other phase in the table registers its check on a listener that is
already bound — but by default the process exits immediately afterwards,
so nothing has time to scrape it.
[`--startup-error-linger`](#keeping-the-endpoints-alive-after-a-failed-startup)
is what makes these entries reachable by a monitoring system rather than
only by an in-process test.

`meta-store` rather than `bolt` names the KV migrations and the
unknown-store-type case because those run for every `--store` value: a
migration failure under `--store=memory` is not a bolt problem, and
there is no bolt file to go and look at. `bolt` names the bolt client
open, which happens only on disk.

Note that an `engine` failure produces two failing entries on **`/ready`**
— `engine` and `shards` — because the startup progress logger latches its
terminal failure into its ready gate. The top-level `message` names
`engine`, which sorts first.

**`/health` gets only the `engine` entry.** The progress logger exposes two
different checks under the one name `shards`, and only the `/ready` one is
driven by `Finish(err)`. The `/health` one reports the errors accumulated
from *individual* shards, and an `engine.Open` that fails before any shard
is loaded has accumulated none — so `shards` reads `"pass"` on `/health`
even while the same name reads `"fail"` on `/ready`. Do not write a
monitoring rule that looks for a failing `shards` on `/health` to detect an
engine problem; look for `engine`. See [`shards`](#shards) for the
`/health` semantics and [`shards` ready
states](#shards-ready-states-progressive) for the `/ready` ones.

Source of truth: `Launcher.failSubsystem` and
`cmd/influxd/launcher/subsystems.go`.

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

> With `--health-auth-enabled` (or `--hardening-enabled`) the `503`
> `checks` array is withheld from a caller who cannot prove operator
> permissions, and carries names and statuses without messages during
> the [startup window](#the-startup-window). `status`, `started` and
> `up` are never withheld.

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
(`kit/check/helpers.go`) while startup is still running. A gate whose
phase failed, or which startup never reached, reports why instead — see
[Gates that failed](#gates-that-failed). The `shards` gate emits
different messages. See below.

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

### Gates that failed

A gate whose phase failed is latched by `Fail(err)` instead, and reports
that error's message in place of `"not ready"` for the rest of the
process. `Fail` outranks both `Ready` and `Unready` and cannot be
cleared: it says the phase will not complete, not that it has not
completed yet. This is startup-only — a runtime degradation monitor must
call `Unready`.

A phase that fails *after* all eight gates have fired — the config
handler, for instance — has no gate to latch, so the launcher registers
a new failing entry named for that phase (see the [startup failure
checks](#startup-failure-checks) table). Without it, `/ready` would go on
reporting `"ready"` right up until the process exits.

**Gates that were never reached are latched too.** When a startup phase
fails, every gate after it is one that will never fire, and `"not ready"`
— which means *not yet* — reads as a server still working through
startup. Before `run` returns, it latches each gate still unfired with
`"not reached: startup failed at <phase>"` (just `"not reached: startup
failed"` if the failure reached no attribution at all). So a failed
startup leaves `/ready` with exactly one entry carrying a reason, the
subsystems that did start still passing, and the rest saying they never
got their turn.

Gates that already fired are skipped. `Fail` outranks `Ready`, so
sweeping a passing gate would report a working subsystem as failed. And
`Fail` keeps the first error, so the phase that actually failed keeps its
own message rather than the generic one.

One consequence worth expecting: a KV migration failure reports as *two*
`/ready` entries, `meta-store` carrying the reason and `bolt` at
`"not reached"`, because the bolt gate is fired by the migrations it
never reached.

`shards` is not a `ReadyGate` and is not swept. On a failure *before*
`engine.Open` — a bad `--store` value, a failed SQL migration, an
unreadable bolt file — it goes on reporting `"waiting for shard
enumeration"` or a load percentage for the life of the process, which
still reads as progress on a server that is on its way out. Read the
entry that carries a reason, not this one.

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

With health auth enabled, this early phase is also the
[startup window](#the-startup-window): until the KV migrations finish
and the authorization store opens, no credential can be checked, and
both endpoints report check names and statuses without their messages.

**They stop serving when the process exits**, which on a failed startup
is immediately — the listener is torn down microseconds after the
failure is recorded. See
[Keeping the endpoints alive after a failed startup](#keeping-the-endpoints-alive-after-a-failed-startup)
for the flag that changes this.

### Keeping the endpoints alive after a failed startup

A startup failure is recorded on both endpoints (see [Startup failure
checks](#startup-failure-checks)), and then the process exits and the
listener goes with it. In practice a monitoring system never sees it:
the body exists for microseconds. The log line is the reliable copy.

`--startup-error-linger` keeps both endpoints answering for a fixed
duration after a failed startup, so a scraper can retrieve which
subsystem failed and why before the process goes away:

```
influxd --startup-error-linger=30s
```

| | default (`0`) | `--startup-error-linger=30s` |
|---|---|---|
| `/health` after a failed start | connection refused | `503`, frozen, naming the failing subsystem, for 30s |
| `/ready` after a failed start | connection refused | `503` `"starting"`, frozen, per-gate reasons, for 30s |
| bolt flock, sqlite, engine | released by process death | released **before** the window opens |
| PID file | released by process death | **held** for the window, released on exit |
| exit code and stderr | `1`, the startup error | unchanged |
| `SIGINT` during the window | — | cuts the window short, then exits |

The equivalent environment variable is
`INFLUXD_STARTUP_ERROR_LINGER`, and the config file key is
`startup-error-linger`. Any Go duration string works (`45s`, `1m`).

**The value is capped at 30 minutes.** The window holds the HTTP port on
a process that has already failed, and every supervisor that would
restart it is waiting on that process to exit, so an unbounded value
turns a failed start into an indefinite outage — a worse failure than the
one the window exists to report. A larger value is accepted, capped, and
logged at `WARN` naming the flag, the value you asked for and the one you
got; `print-config` still reports what you configured.

**Everything except the listener and the PID file is released before the
wait begins.** The bolt flock, the sqlite file and the engine directory
belong to the next run rather than to one that already failed, so a
supervisor with `Restart=on-failure` is not blocked for the length of the
window.

The PID file is deliberately *not* released with them. It is the
interlock that stops a second `influxd` starting against this data
directory, and for the length of the window this process is still running
and still holding its port — so releasing it would let a concurrent start
past the check that exists to catch exactly this, only to fail it later
on `listen tcp: address already in use`, which names the wrong cause. A
PID file describes a live process for as long as the process is alive.
It is removed by the final teardown, after the listener closes.

One exception, which predates this flag: an engine that failed partway
through `Open` registers no closer, so it is not closed here either.
Nothing holds a lock on it in that state.

**The report is frozen at the moment of failure.** Tearing a subsystem
down makes its own check start failing — a closed sqlite handle fails its
ping, and the `bolt` prober's last result ages into `"stale: last probe
…"` — and because failing checks sort first and `/health`'s top-level
`message` is the first of them, a closed `bolt` would otherwise outrank
and mask the `engine` failure the window exists to publish. So the whole
check set is snapshotted before any teardown runs and served unchanged
for the rest of the process's life.

What is frozen is the *check set*, not the whole envelope. Two `/health`
scrapes 30 seconds apart return byte-identical documents: every field it
carries — `status`, `message`, `checks`, `version`, `commit` — comes from
the frozen set or from build info. `/ready` additionally reports
`started` and `up`, and `up` is recomputed per request as the elapsed
time since the handler was built, so it advances across the window like
it does at any other time. A scraper diffing `/ready` bodies to decide
whether the report has changed must ignore `up`; the `checks` array is
the part that is pinned.

`/ready` reports `"starting"` throughout, exactly as it does during a
normal boot. `/health` is the endpoint that distinguishes the two: it
passes for the whole of a normal startup and fails only once a phase has
failed.

> [!IMPORTANT]
> `/health` returns `503` from the **start** of the window. A liveness
> probe whose `periodSeconds × failureThreshold` is shorter than the
> linger will kill the container before anyone scrapes the reason, and
> the feature will appear to work in manual testing and silently not in
> production. Nothing in this repository configures a probe — those live
> in Helm charts and operator manifests — so check yours before choosing
> a value. A startup probe with a generous `failureThreshold`, or a
> liveness probe that does not start until the startup probe succeeds, is
> the usual arrangement.

> [!WARNING]
> `SIGTERM` is not trapped. `influxd` registers only `os.Interrupt`, so a
> `systemctl stop` or a pod deletion during the window kills the process
> where it stands and the final teardown never runs. The split above is
> what limits the damage: the file locks are already gone, so the next
> start is not blocked on them. What is left behind is a stale PID file —
> the same thing an uncatchable signal leaves behind at any other point in
> the process's life, and what `--overwrite-pid-file` is for. `SIGINT`
> (Ctrl-C) does cut the window short.

With health auth enabled the window is less useful than it looks; see
[Check detail during the window](#check-detail-during-the-window).

**One behavior change at the default.** `Shutdown` now runs on the
startup-failure path even at `--startup-error-linger=0`. It did not
before: a failed startup returned without reaching it, so a `--pid-file`
was left behind and the next start met
`PID file exists (possible unclean shutdown or another instance already
running)`. That is now cleaned up.

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

**Meaning:** The engine hasn't yet begun calling `AddShard`. The engine
is either still in early initialization or blocked before enumeration.

**Action:** Look for the `engine` gate also failing with `"not ready"`,
which indicates early initialization. Check logs for engine open progress.

### `/ready` 503 — terminal shard load failure

```json
{
  "status": "starting",
  "checks": [
    { "name": "shards", "status": "fail", "message": "shard loading failed: open /var/lib/influxdb2/engine/data/2/.../000000001-000000001.tsm: input/output error" }
  ]
}
```

**Meaning:** `engine.Open` returned an error and the `shards` gate
latched into a terminal failure. Restarting won't clear this without
addressing the underlying error in the message.

**Action:** Check disk health and file permissions on the data directory.
Address the underlying error before you restart.

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

**Meaning:** The background bolt prober hasn't recorded a fresh result
within the 5-second staleness budget. Either the prober goroutine is
wedged in a `db.View` and the bolt mmap is unresponsive, or the host is
under severe scheduling pressure that prevents the 1-second probe loop
from running.

**Action:** Check disk and kernel logs for an unresponsive mmap. Inspect
`iostat`, `dmesg`, and the `influxd` process's CPU and memory state.

**With `--health-auth-enabled`,** this is the one failure where an
operator token buys nothing: resolving it would read the store that is
wedged. The body above loses its `message` fields for every caller and
keeps the `checks` names and statuses, so you can still see what is
failing here and in the other subsystems — see
[Behavior when the KV store is wedged](#behavior-when-the-kv-store-is-wedged).

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

### `/health` 503 — a startup phase failed

```json
{
  "status": "fail",
  "message": "Failed to open engine: mkdir /var/lib/influxdb2/engine: permission denied",
  "checks": [
    { "name": "bolt",   "status": "pass" },
    { "name": "engine", "status": "fail",
      "message": "Failed to open engine: mkdir /var/lib/influxdb2/engine: permission denied" },
    { "name": "shards", "status": "pass" },
    { "name": "sqlite", "status": "pass" }
  ]
}
```

**Meaning:** Initialization failed at the named phase and the process is
on its way out. The message is the error verbatim, prefixed by what was
being attempted. The corresponding `/ready` entry carries the same
message in place of `"not ready"`, and a phase with no gate of its own
adds a new failing `/ready` entry (see [Gates that
failed](#gates-that-failed)).

Only the failing phase is named here. `shards` stays `"pass"` on
`/health` — it reports per-shard load errors, and an engine that never
opened loaded no shards — while the same gate reads `"fail"` on `/ready`
with `"shard loading failed: …"` — see [`/ready` 503 — terminal shard load
failure](#ready-503--terminal-shard-load-failure).

**Action:** Read the message — it is the same error the log carries,
with a `subsystem` field naming the phase. This state is terminal: the
process exits rather than retrying, so an orchestrator restarting the
container will hit the same failure until the underlying cause is fixed.

**Note on timing:** by default `influxd` exits as soon as `run` returns,
so a scraper will almost certainly not catch the body before the listener
closes — the log line is the reliable copy. Set
[`--startup-error-linger`](#keeping-the-endpoints-alive-after-a-failed-startup)
to hold both endpoints open, with this body frozen, long enough to be
scraped.

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

**Meaning:** `db.PingContext` didn't return within the 500ms probe
deadline, or it returned an I/O error.

**Action:** Check disk health on the SQLite database file and whether
another process is holding a long-running write transaction.

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

**Meaning:** One or more shards failed to load during engine open. Errors
accumulate. Even if the engine opened successfully, `/health` continues
to report this until restart.

**Action:** Each `shard <id>` in the message identifies a specific shard
directory under the configured data path. Address each shard's underlying
error before you restart.

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

- `http/check_handler.go` — endpoint handler, JSON envelopes, the
  authorization gate and the reduced body.
- `http/check_handler_jsoncompat_test.go` — pinned wire format.
- `http/check_handler_auth_test.go` — permission matrix and reduced-body
  wire format.
- `http/authentication_middleware.go` — `CredentialResolver`, the
  `Authorize` method the gate calls.
- `http/handler.go` — `HealthPath`, `ReadyPath` constants,
  `serverHeaderWriter`.
- `authz.go` — `OperPermissions`, `Permission.matchesV1` and the
  `instance` wildcard.
- `cmd/influxd/launcher/cmd.go` — `--health-auth-enabled`,
  `--hardening-enabled`, and `applyHardeningImplications`, which lets an
  explicitly-set `--health-auth-enabled` override the implication;
  `resolveOptions`, the `PreRunE` that applies it for the server and
  `print-config` alike.
- `cmd/influxd/launcher/launcher.go` — `openMetaStores` /
  `migrateSQLStore` and the credential-resolver install between them.
- `kit/check/check.go` — status enum, aggregation rule.
- `kit/check/freshness.go` — staleness model and stale-message format.
- `kit/check/helpers.go` — `ReadyGate` (including `Fail`),
  `DefaultProbeTimeout`.
- `cmd/influxd/launcher/launcher.go` — `failSubsystem` and
  `initReadyChecks`, which produce the startup failure checks.
- `cmd/influxd/launcher/subsystems.go` — the canonical check names.
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
- `cmd/influxd/launcher/health_ready_auth_test.go` — end-to-end
  authorization behavior.
