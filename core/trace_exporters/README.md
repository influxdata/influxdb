# Trace Exporters

## Regenerating Jaeger Thrift

_The instructions below use docker, but this is optional._

_Depending on your setup there may be permissions complications that require using`-u`_

Startup a Debian bookworm image

```
docker run -it -v $PWD:/out debian:bookworm-slim
```

Install the thrift-compiler

```
$ apt-get update
$ apt-get install thrift-compiler wget
```

Verify the version of the compiler matches the version of `thrift` in [Cargo.toml](./Cargo.toml)

```
$ thrift --version
Thrift version 0.13.0
```

Get the IDL definition

```
$ wget https://raw.githubusercontent.com/jaegertracing/jaeger-idl/master/thrift/jaeger.thrift https://raw.githubusercontent.com/jaegertracing/jaeger-idl/master/thrift/zipkincore.thrift https://raw.githubusercontent.com/jaegertracing/jaeger-idl/master/thrift/agent.thrift
```

Generate the code

```
$ thrift --out /out/src/thrift --gen rs agent.thrift
$ thrift --out /out/src/thrift --gen rs jaeger.thrift
$ thrift --out /out/src/thrift --gen rs zipkincore.thrift
```

Patch up imports

```
sed -i 's/use jaeger;/use super::jaeger;/g' /out/src/thrift/agent.rs
sed -i 's/use zipkincore;/use super::zipkincore;/g' /out/src/thrift/agent.rs
```

Remove the clippy line

```
#![cfg_attr(feature = "cargo-clippy", allow(too_many_arguments, type_complexity))]
```
