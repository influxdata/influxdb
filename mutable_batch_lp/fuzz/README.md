# Fuzz tests

The fuzz tests in this `fuzz` crate were created using [cargo-fuzz] version 0.11.3.

[cargo-fuzz]: https://rust-fuzz.github.io/book/introduction.html

## One-time setup

To install `cargo-fuzz`:

```
$ cargo install cargo-fuzz
```

You'll also need a nightly Rust:

```
$ rustup install nightly
```

## Running

To run an existing fuzz test, change to the `mutable_batch_lp` directory and run:

```
$ cargo +nightly fuzz run <TARGET>
```

where `<TARGET>` is the name of one of the files in `fuzz/fuzz_targets`. To list all targets, run:

```
$ cargo fuzz list
```

## Adding more

To add more fuzzing targets, run:

```
$ cargo fuzz add <TARGET>
```

which will add a new file in `fuzz/fuzz_targets`. Edit the new file to call the code you want to
fuzz; see the [`cargo-fuzz` tutorial] for examples.

[`cargo-fuzz` tutorial]: https://rust-fuzz.github.io/book/cargo-fuzz/tutorial.html
