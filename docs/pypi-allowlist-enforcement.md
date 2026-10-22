# InfluxDB 3 Server-side PyPI allowlist

## Goal

Manage InfluxDB 3 cluster dependencies by restricting which Python
packages end users may install through the Processing Engine. A user runs
`influxdb3 install package numpy==2.1.3`; the install succeeds only if that exact
`name==version` is on an operator-controlled allowlist. Anything else is rejected
with a clear HTTP 403. Integrity is enforced server-side with a pip constraints
file whose enforcement mode (`--plugin-package-constraints-mode`) selects version pinning or
hash verification. No CLI change is needed. Enforcement is entirely server-side,
so users cannot bypass it, including via the requirements (`-r`) path.

## Request flow (unchanged plumbing)

```
influxdb3 install package <spec>            (CLI, influxdb3_commands/src/install.rs)
  -> POST /api/v3/configure/plugin_environment/install_packages
  -> http.rs::install_plugin_environment_packages
  -> PythonEnvironmentManager::install_packages(packages)        <-- enforcement point

influxdb3 install package -r <file>
  -> POST /api/v3/configure/plugin_environment/install_requirements
  -> http.rs::install_plugin_environment_requirements
  -> PythonEnvironmentManager::install_requirements(path)        <-- enforcement point
  -> pip
```

Both install paths are enforced inside the `PythonEnvironmentManager`, before pip
is invoked. The client sends the user's raw specs as `packages: Vec<String>`
(direct path) or a `requirements_location` path (the `-r` path). The existing
`handle_install_result` in `http.rs` already maps the "installation disabled"
error class to HTTP 403 with the error string as the body; the new rejections
reuse that path, so no HTTP-layer change is required.

### Authentication / authorization context

The install endpoints sit behind InfluxDB's existing token auth and are
admin-gated (the same `authorize_admin` path as other plugin routes). The
allowlist is a layer *behind* that gate and is **identity-blind**: it keys on
`name==version` only and applies uniformly to every caller.

## Design

Two independent controls, both owned by the server:

1. **Allowlist (authorization)**, set by `--plugin-package-allowlist`. A file
   of pinned `name==version` entries. The server validates every requested spec
   against it and rejects anything not present. This answers "is the user allowed
   to install this?"
2. **Constraints + hashes (integrity)**, set by `--plugin-package-constraints` and
   `--plugin-package-constraints-mode`. A server-controlled pip constraints file, appended
   to every pip invocation as `-c <file>`. The mode selects enforcement: `pin`
   for version pinning, or `require-hashes` to also pass `--require-hashes`. This
   answers "is the artifact that pip resolves the one we expect?" Hashes cover
   the full transitive closure.

The controls are orthogonal: the allowlist gates the user-facing request; the
constraints file governs what pip actually downloads. Both are configured by the
operator and neither is reachable by the end user.

## Changes

Five files, four crates.

### 1. `influxdb3_clap_blocks/src/plugins.rs`: configuration

Three fields added to `ProcessingEngineConfig`, plus a `PipConstraintsMode` enum:

| Flag | Env | Type | Purpose |
|---|---|---|---|
| `--plugin-package-allowlist` | `INFLUXDB3_PLUGIN_PACKAGE_ALLOWLIST` | `Option<PathBuf>` | Path to the allowlist file. Unset = no allowlist check. |
| `--plugin-package-constraints` | `INFLUXDB3_PLUGIN_PACKAGE_CONSTRAINTS` | `Option<PathBuf>` | Passed to pip as `-c <file>`. |
| `--plugin-package-constraints-mode` | `INFLUXDB3_PLUGIN_PACKAGE_CONSTRAINTS_MODE` | `pin` \| `require-hashes` (default `pin`) | How the constraints file is enforced. `pin` = version-pin only. `require-hashes` additionally passes `--require-hashes`; requires `--plugin-package-constraints` or startup fails. |

### 2. `influxdb3_processing_engine/src/environment.rs`: core logic

- **`AllowlistedPackages`**: the parsed allowlist. Loads from file (blank lines and
  `#` comments skipped; every other line must be a pinned `name==version`, else a
  line-numbered parse error). Names are PEP 503-normalized, so `Foo_Bar` and
  `foo-bar` match. `validate_spec` returns the specific rejection for a requested
  spec; unpinned names and range operators (`>=`, `~=`) are rejected.
- **`PipPolicy`**: the server-side policy: optional allowlist, optional
  constraints file, and whether hashes are required. Built once at startup via
  `from_config`, which parses the allowlist eagerly (bad file fails startup) and
  rejects `require-hashes` without a constraints file. Produces the pip args
  (`-c <file>` and/or `--require-hashes`) and validates install requests.
- **`PipManager`** holds a `PipPolicy`. Both install paths validate against the
  policy *before* invoking pip and now surface a non-zero pip exit as an error
  (previously ignored):
  - `install_packages`: validates each spec against the allowlist.
  - `install_requirements`: validates the `-r` file (see below).

#### Requirements-path enforcement

`install_requirements` (the `-r requirements.txt` path) is validated server-side
so it cannot bypass the allowlist. The server reads the file and rejects any line
that is not a plain pinned requirement: line continuations, pip options (`-e`,
`--index-url`, `-r`, `-c`, `--hash`, …), URLs/VCS refs, `name @ url` references,
local paths, and environment markers. Each surviving line goes through the same
`validate_spec` as `install_packages`. A policy with no allowlist performs no
check (unchanged behavior).

### 3. `influxdb3_processing_engine/src/manager.rs`: error routing

The allowlist rejections (`PackageNotAllowlisted`, `PackageVersionNotAllowlisted`,
`PackageVersionRequired`, `RequirementsEntryNotAllowed`) route through
`ProcessingEngineError::PackageInstallationDisabled`, which `http.rs` already
maps to **HTTP 403** with the message as the body. A file read error is a server
error (not 403); a `require-hashes`-without-constraints misconfiguration fails at
startup and never reaches a request.

### 4. `influxdb3/src/commands/serve.rs`: wiring

`setup_processing_engine_env_manager` builds the `PipPolicy` from config at
startup (fail-fast on a bad allowlist file or `require-hashes` without a
constraints file) and hands it to the `PipManager`.

### 5. `influxdb3/src/commands/serve/tests.rs`: test fixture

Updated for the new config fields and the now-`Result` return.

## Rejection messages (HTTP 403)

| Case | Message |
|---|---|
| Package not on list | `'<name>' is not in the package allowlist. Contact your administrator to request that it be added to the allowlist.` |
| Wrong version | `'<name>==<req>' is not supported. Allowlisted version(s): <list>.` |
| Missing version | `'<name>' must be installed with a pinned version, e.g. '<name>==<ver>'.` |
| Disallowed requirements entry (`-r`) | `requirements file entry on line <n> is not allowed: <reason> ('<line>')` |

On the `-r` path, a line that *is* a plain pinned requirement but is not on the
allowlist produces the same not-on-list / wrong-version / missing-version
messages above (it runs through the same `validate_spec`).

## Maintaining the allowlist

The allowlist is a plain text file the operator owns; there is no API to mutate
it. Format:

```
# comments and blank lines are ignored
numpy==2.1.3
numpy==2.0.2        # multiple approved versions of one package: one per line
pandas==2.2.0
```

Rules:
- Each entry must be a pinned `name==version`. Unpinned names and range
  operators (`>=`, `~=`, …) are rejected at load time with a line number.
- Names are matched PEP 503-normalized, so casing and `-`/`_`/`.` differences do
  not matter.
- Multiple versions of a package are allowed; list each on its own line.

Operational model:
- **Add/remove a package or version:** edit the file, then restart the server (or
  reload config) so it is re-read and re-validated. A malformed file fails
  startup fast rather than silently disabling enforcement.
- **Pin integrity:** keep a companion constraints file generated with
  `pip-compile --generate-hashes` and set `--plugin-package-constraints` +
  `--plugin-package-constraints-mode require-hashes`. The constraints file must include
  hashes for transitive dependencies too. Point pip at a private index
  (`--index-url`) if you require a vetted mirror. Use the default
  `--plugin-package-constraints-mode pin` if you only need version pinning without hashes.
- **Least privilege:** run the engine as a user without write access to the venv
  and the allowlist/constraints files. Use `--disable-package-management` to
  freeze installs entirely after provisioning.

### Constraints file format

The constraints file is a standard pip constraints file (`-c`), not an InfluxDB
format. It governs the *full resolved closure* (including transitive deps), not
just the packages a user names.

With `--plugin-package-constraints-mode pin`, version pins only:

```
# constraints.txt
numpy==2.1.3
python-dateutil==2.9.0.post0
pytz==2024.1
```

With `--plugin-package-constraints-mode require-hashes`, every entry (direct and
transitive) must carry a hash, usually generated with
`pip-compile --generate-hashes`:

```
# constraints.txt (generated: pip-compile --generate-hashes)
numpy==2.1.3 \
    --hash=sha256:0346186660e611ea33e1a1a4b2ef0bc5b24a1dd63e4e8d6a97eb4e3b4e1a0b1a \
    --hash=sha256:5a1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef
python-dateutil==2.9.0.post0 \
    --hash=sha256:11111111111111111111111111111111111111111111111111111111111111aa
```

The hashes above are illustrative placeholders; real hashes come from
`pip-compile --generate-hashes` (or `pip hash`).

### Operator-defined alternate index (e.g. CPU-bound torch)

A constraints file can carry an `--index-url`, so the operator can point pip at a
specific index without the end user ever supplying one. End users never pass
`--index-url` (it is not a CLI arg, and the `-r` path rejects index options); the
index is entirely operator-controlled.

For example, to allow the CPU build of torch from the PyTorch CPU index:

```
# constraints.txt
--index-url https://download.pytorch.org/whl/cpu
torch==2.4.0+cpu
```

```
# allowlist.txt
torch==2.4.0+cpu
```

The user then runs:

```
influxdb3 install package torch==2.4.0+cpu
```

The allowlist matches the exact pin (the `+cpu` local-version segment is part of
the version string and is compared verbatim), and the constraints file redirects
pip to the CPU index.

## File permissions & isolation

Plugins run **in-process as the server's OS user**, so file permissions only
protect the allowlist/constraints files when they are owned by a *different*
user than the one the server runs as. Make them admin-owned and read-only:

```
sudo chown root:root allowlist.txt constraints.txt
sudo chmod 0444 allowlist.txt constraints.txt   # r--r--r--
# also ensure the containing directory is not writable by the server user,
# so the files cannot be deleted and recreated
```

Requirements for this to hold:
- The server runs as a **non-root** user (root ignores permission bits and could
  rewrite a `0444` file it does not own).
- That user is **not the owner** of the files (an owner can `chmod` the file back
  to writable).

The files are also read and parsed **once at startup** into an in-memory policy;
enforcement never re-reads from disk. So any out-of-band edit to a file has no
effect until the server is restarted.

## Known gaps / follow-ups

- **Unpinned requests are always rejected, even if PyPI's latest is approved.**
  Validation is purely lexical on the spec the user typed and never queries PyPI.
  A bare `numpy` (no `==version`) always fails with
  `PackageVersionRequired` (HTTP 403), even when PyPI's current latest happens to
  be a version that is on the allowlist. This is intentional: "latest" is a
  moving target, so resolving it would make the same command's success
  time-dependent. Users must pin an approved `name==version`.
- The allowlist is validated but not hot-reloaded; changes require a restart.
- The requirements (`-r`) path accepts only plain pinned `name==version` lines
  when an allowlist is set; it does not support URLs, VCS refs, editable
  installs, index options, or environment markers. This is intentional (those
  forms can pull unapproved code), but means an operator cannot use those
  features together with an allowlist.

## PoC summary

The proof of concept implements the full control described above and validates it
end to end:

- **Built:** the three config flags, the server-side allowlist and
  constraints/mode policy, enforcement on both the direct (`install_packages`)
  and requirements (`install_requirements`) install paths, and HTTP 403 mapping
  for rejections. No CLI or HTTP-layer changes were required.
- **Unit tested:** 26 tests covering name normalization, spec parsing, allowlist
  parsing and validation, the `require-hashes` fail-fast rule, and the
  requirements-file validation matrix (URLs, VCS refs, pip options, local paths,
  markers, line continuations, unpinned/wrong/unapproved entries, and the
  no-allowlist no-op).
- **Validated end to end** against a running server with an allowlist of
  `numpy==2.1.3`:
  - `numpy==2.1.3` (allowlisted) installs (verified imported from the venv).
  - `numpy==2.5.3` (latest, not allowlisted) rejected with 403.
  - `scipy==1.11.0` (not listed) rejected with 403.
  - `numpy` (unpinned) rejected with 403.
