# Valgrind
This document explains how to use [Valgrind] to perform certain debug tasks.

## Build
Create a debug build that uses the system memory allocator (i.e. neither [heappy] nor [jemalloc]):

```console
$ cargo build --no-default-features
```

## Memory Leaks
There is a script that does most of the config setting. Just start the server with:

```console
$ ./scripts/valgrind_leak ./target/debug/influxdb_iox run ...
```

You can kill the server w/ `CTRL-C` when you're ready. The [Valgrind] output will be written to `valgrind-out.txt`.

## Suppression Rules
[Valgrind] allows you to suppress certain outputs. This can be used to ignore known "issues" like that [lazycell] leaks.
For IOx we provide a file that is used by the scripts (under `scripts/valgrind.supp`). If you plan to write your own
rules, here are some useful links:

- <https://valgrind.org/docs/manual/mc-manual.html#mc-manual.suppfiles>
- <https://www.valgrind.org/docs/manual/manual-core.html#manual-core.suppress>
- <https://wiki.wxwidgets.org/Valgrind_Suppression_File_Howto>

You may also use the `--gen-suppressions=all` to auto-generate supppression rules:

```console
$ ./scripts/valgrind_leak --gen-suppressions=all ./target/debug/influxdb_iox run ...
```

Note that Rust symbols like `influxdb_iox::main` are mangled in a way that [Valgrind] cannot parse them (e.g. to
`_ZN12influxdb_iox4main17h940b8bf02831a9d8E`). The easiest way is to replace `::` w/ `*` and prepand and append an
additional wildcard `*`, so `influxdb_iox::main` gets `*influxdb_iox*main*`.

[heappy]: https://github.com/mkmik/heappy
[jemalloc]: ttps://github.com/jemalloc/jemalloc
[lazycell]: https://crates.io/crates/lazycell
[Valgrind]: https://valgrind.org/
