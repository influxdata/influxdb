## `ingester2` benchmarks

Run them like this:

```console
% cargo bench -p ingester2 --features=benches
```

This is required to mark internal types as `pub`, allowing the benchmarks to
drive them.
