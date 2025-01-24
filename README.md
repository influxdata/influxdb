# InfluxDB Enterprise
This is a fork of Core with all the Enterprise functionality. To bring in the latest changes from Core, clone this repo then do:

## Core to Enterprise Code Sync

```
# add the Core repo as a remote
git remote add core git@github.com:influxdata/influxdb.git
git fetch core
git checkout -b pd/oss-sync
git merge core/main
# fix any conflicts and commit
git push --set-upstream origin pd/oss-sync
```

Then create a PR to merge `pd/oss-sync` into `main`. Use a merge PR that pulls over the commits.

## Reducing merge conflicts with code from core

When writing new code for Enterprise (this repository), it is ideal to keep as much of it separated
from core code as possible, so when performing the above [Core to Enterprise Code Sync](#core-to-enterprise-code-sync),
you reduce the chance of code conflicts. These can sometimes be difficult to reason about,
so is best to avoid them if at all possible.

Here are some strategies for minimizing conflicts.

### Create a new crate

If the code you are writing can stand on its own, then create a new crate for it in the
[`influxdb3_enterprise`](/influxdb3_enterprise) folder.

In its `Cargo.toml`, the package name should be of the following form.

```toml
[package]
name = "influxdb3_enterprise_compactor"
```

Then, add that crate's package name to the `[workspace.members]` list in the workspace `Cargo.toml`
file at the root of the repository.

### Extend an existing core crate with an `enterprise.rs` module

If the code needs to be written in one of the existing core crates, because, e.g., you are relying
on crate public APIs in that code that is not visible from other crates, then use this strategy.

Create a module called `enterprise.rs` at whatever level is appropriate for the module you are extending
and include it at the top of that module.

For example, if you are extending the `MyStruct` type with enterprise-specific methods, then, in the
module `MyStruct` is defined in, add the `enterprise` module:

```rust
// my_module/mod.rs
pub mod enterprise;

pub struct MyStruct;

impl MyStruct {
  /* methods available in core */
}
```

Then in the new `enterprise.rs` module you created, add the enterprise-specific code:
```rust
// my_module/enterprise.rs

impl MyStruct {
  pub fn fancy_paid_feature() {
    /* ... */
  }
}
```

Now, `MyStruct::fancy_paid_feature` can be called from the enterprise codebase.

You can see several examples of this throughout the codebase:

- In the Query Executor ([link](/influxdb3_server/src/query_executor/enterprise.rs))
- In the Catalog ([link](/influxdb3_catalog/src/catalog/enterprise.rs))
- In the HTTP server ([link](/influxdb3_server/src/http/enterprise.rs))
- in the Write Buffer ([link](/influxdb3_write/src/write_buffer/enterprise.rs))

## influxdb3_core_internal dependency

Enterprise depends on both `influxdb3_core` and `influxdb3_core_internal` (closed source crates). And also, `influxdb3_core_internal`
itself depends on `influxdb3_core` for common utils (like `iox_time`). When amending `influxdb3_core` version/revision
`influxdb3_core_internal` needs to be on the same version/revision as well.

### CircleCI

- To be able to pull `influxdb3_core_internal` crates, SSH key has been added to CircleCI. Public key is added to
  `influxdb3_core_internal` repo. The private key is added to the [engineering vault](https://team-influxdata.1password.com/app#/gy77wv6esuba5hoknu3apl4mlu/Tag?itemListId=yruguqzv7uyenepybxltzh23ei)
  in 1password.

- In order to revoke or rotate the keys, it should be as simple as deleting the keys in CircleCI and in Github and
  reinstating new keys following the [docs](https://github.com/influxdata/docs.influxdata.io/blob/main/content/development/security/kb.md#circleci-and-ssh-keys)

