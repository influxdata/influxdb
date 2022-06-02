# sqlx-hotswap-pool

This crate implements a workaround for the lack of support for password rotation in the `sqlx` crate.

There is an upstream ticket for this [Support rotating passwords #445](https://github.com/launchbadge/sqlx/issues/445).
This crate offers a more quick&dirty solution to the problem.

## Problem

Some authentication methods for databases provide short lived passwords that must be regularly rotated.

Examples are: [AWS IAM database authentication](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.html), HashiCorp Vault's dynamic role, ...

However, in `sqlx` once you create a pool you need to pass the connection string (which includes the credentials) and you can't change it afterwards.
The pool will create one or more connections with those credentials.

## Workaround

This crate implements a wrapper struct around a reference counted Pool smart pointer. This wrapper can then be updated using internal mutability (mutex protected) whenever the main binary detects a credentials refresh. Every subsequent use of the pool will use the new underlying pool.

This workaround has been designed to solve the problem of updating credentials, but it happens to work if you want to point your pool to an entirely different database as well.

If the credentials refresh happen before the existing credentials are invalidated, references to the previous pool can still be used for some time.

If the credentials refresh contextually invalidates the existing credentials, the process will experience connection errors if they used the pool before it has been updated (and if they cloned the `Arc` before the `update` method has been called).

Already open connections will keep working in both cases.

Usage:

```rust
use sqlx_hotswap_pool::HotSwapPool;
use sqlx::{pool::PoolOptions, Pool, Postgres};
# async fn foo() {
let pool1: Pool<Postgres> = PoolOptions::new()
    .test_before_acquire(true)
    .connect("postgresql://user:pw1@localhost/db")
    .await
    .unwrap();

// create a HotSwapPool, a pool that wraps `pool1` and supports replacing it with another
let pool: HotSwapPool<Postgres> = HotSwapPool::new(pool1);

let pool2 = PoolOptions::new()
    .test_before_acquire(true)
    .connect("postgresql://user:pw2@localhost/db")
    .await
    .unwrap();
    
// replace the pool wrapped by the HotSwapPool with `pool2` instead of `pool1`
pool.replace(pool2);
# }
```
