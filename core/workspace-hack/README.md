# workspace-hack

This crate is a "workspace hack" crate managed by [`cargo hakari`][hakari].

Its purpose is to unify the features used by all crates in the workspace so that the crates share
more dependencies and rebuild crates less. There are more details in [hakari's
documentation][hakari-docs].

[hakari]: https://crates.io/crates/cargo-hakari
[hakari-docs]: https://docs.rs/cargo-hakari/0.9.6/cargo_hakari/about/index.html

## CI failures

If the `workspace_hack_checks` CI job is failing, there are two possible reasons and solutions:

- If `cargo hakari generate --diff` fails, that means a crate has started or stopped using a
  feature of some crate and that feature isn't up-to-date in the `workspace-hack` crate. To fix
  this, run `cargo hakari generate` and commit the changes.
- If `cargo hakari manage-deps --dry-run` fails, that means a crate in the workspace isn't
  depending on the `workspace-hack` crate. To fix this, run `cargo hakari manage-deps` and commit
  the changes.
