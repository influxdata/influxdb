# Rationale and Goals
As every Rust programmer knows, the language has many powerful features, and there are often several patterns which can express the same idea. Also, as every professional programmer comes to discover, code is almost always read far more than it is written.

Thus, we choose to use a consistent set of idioms throughout our code so that it is easier to read and understand for both existing and new contributors.



## Errors

### All errors should follow the [SNAFU crate philosophy](https://docs.rs/snafu/0.6.8/snafu/guide/philosophy/index.html) and use SNAFU functionality

*Good*:

* Derives `Snafu` and `Debug` functionality
* Has a useful, end-user-friendly display message 

```rust
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(r#"Conversion needs at least one line of data"#))]
    NeedsAtLeastOneLine,
    // ...
}
```

*Bad*:
```rust
pub enum Error {
    NeedsAtLeastOneLine,
    // ...
```



### Use the `ensure!` macro to check a condition and return an error

*Good*:

* Reads more like an `assert!`
* Is more concise
```rust
ensure!(!self.schema_sample.is_empty(), NeedsAtLeastOneLine);
```

*Bad*
```rust
if self.schema_sample.is_empty() {
    return Err(Error::NeedsAtLeastOneLine {});
}
```


### Errors should be defined in the module they are instantiated



*Good*:

* Groups related error conditions together most closely with the code that produces them
* Reduces the need to `match` on unrelated errors that would never happen

```rust
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Not implemented: {}", operation_name))]
    NotImplemented { operation_name: String }
}
// ...
ensure!(foo.is_implemented(), NotImplemented {
    operation_name: "foo",
}
```

*Bad*
```rust
use crate::errors::NotImplemented;
// ...
ensure!(foo.is_implemented(), NotImplemented {
    operation_name: "foo",
}
```

### The `Result` type alias should be defined in each module

*Good*:

* Reduces repetition
```
pub type Result<T, E = Error> = std::result::Result<T, E>;
...
fn foo() -> Result<bool> { true }
```

*Bad*
```
...
fn foo() -> Result<bool, Error> { true }
```



### `Err` variants should be returned with `fail()`

*Good*
```rust
return NotImplemented {
    operation_name: "Parquet format conversion",
}.fail();
```

*Bad*
```rust
return Err(Error::NotImplemented {
    operation_name: String::from("Parquet format conversion"),
});
```


### Use `context` to wrap underlying errors into module specific errors

*Good*:

* Reduces boilerplate

```rust
input_reader
    .read_to_string(&mut buf)
    .context(UnableToReadInput {
        input_filename,
    })?;
```

*Bad*

```rust
input_reader
    .read_to_string(&mut buf)
    .map_err(|e| Error::UnableToReadInput {
        name: String::from(input_filename),
        source: e,
    })?;
```

### Each error cause in a module should have a distinct Error enum

Specific error types are preferred over a generic error with a `message` or `kind` field.

*Good*:

- Makes it easier to track down the offending code based on a specific failure
- Reduces the size of the error enum (`String` is 3x 64-bit vs no space)
- Makes it easier to remove vestigial errors
- Is more concise

```rust
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error writing remaining lines {}", source))]
    UnableToWriteGoodLines { source: IngestError },

    #[snafu(display("Error while closing the table writer {}", source))]
    UnableToCloseTableWriter { source: IngestError },
}

// ...

write_lines.context(UnableToWriteGoodLines)?;
close_writer.context(UnableToCloseTableWriter))?;
```


*Bad*

```rust
pub enum Error {
    #[snafu(display("Error {}: {}", message, source))]
    WritingError {
        source: IngestError ,
        message: String,
    },
}

write_lines.context(WritingError {
    message: String::from("Error while writing remaining lines"),
})?;
close_writer.context(WritingError {
    message: String::from("Error while closing the table writer"),
})?;
```
