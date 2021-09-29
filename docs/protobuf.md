# Protobuf

[Protocol Buffers](https://github.com/protocolbuffers/protobuf) (a.k.a., protobuf)
are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data.

IOx makes extensive use of protobuf, for both its gRPC APIs and also persisting configuration and the catalog

## Guidelines

Some general guidelines, in addition to those enforced by [buf lint](https://docs.buf.build/lint/overview):

* Only use [proto 3] syntax
* When rendering protobuf for CLIs, etc... prefer the [JSON](#json) representation over others (e.g. `std::fmt::Debug`)
* Clearly document any additional semantics attached to a field's [default value](#nullability--presence--defaults)

## Nullability / Presence / Defaults

### Background

Nullability refers to the ability for a field to be present or not. In Rust nullability is typically represented
by `Option`, in other languages pointers are often used. For example in Rust:

* Non-nullable unsigned 32-bit integer -> `u32`
* Nullable string -> `Option<String>`
* Non-nullable unsigned 32-bit integer array -> `Vec<u32>`
* Nullable map -> `Option<BTreeMap<_,_>>`

Nullability allows a client to distinguish between a field specified to have the default value, and an unspecified
field. Without nullability there is no way to represent this additional presence information.

### Proto 3

In [proto 3] the only nullable fields are those that are message-typed or contained within a [oneof]. All other fields
are **always** present in the decoded representation, but potentially initialized to that type's [default value]. This
is irrespective of if the producer was even aware of the field's existence at all.

This is unlike JSON where all fields are nullable, and unlike proto 2 where all except maps and repeated fields are
nullable.

This was an intentional [design decision] by the maintainers of protobuf, in part to improve the ergonomics of the
generated code. This allows code generators to remove often redundant nullability from the generated code - the golang
protoc plugin uses this to avoid a pointer indirection, and similarly the Rust code generator `prost` uses this to
avoid wrapping everything in `Option`

It might be surprising that nullability is restricted to certain fields, with other fields having an implicit
[default value]. However, it is worth noting that these default values are static, and in most cases are the same as the
language defaults, e.g. zero value for numeric types, empty string, etc... and so perhaps this isn't all that unusual

### Special Values

In many cases there will be special semantics attached to the [default value] of a field, it is important that these are
clearly documented, much like any constraint on what constitutes valid data for that field. Examples might be:

* "a value of 0 will use the server-side default"
* "a value of 0 implies no limit"
* "this string must not be empty"
* "this must be a non-zero integer less than 500"
* "this must be a non-empty string containing only alphanumeric characters"

### Explicit Presence

_In most cases it is possible to structure APIs to avoid needing this functionality, but it is documented for
completeness. Think twice before using this functionality, as it introduces a non-trivial amount of cognitive
complexity, both because documentation for it is fairly limited, and also the feature overloads the term optional_

In some cases there is a need for a scalar field to **both** take on the field's [default value] and for this not to be
the default behaviour.

An example might be a limit where we want to apply a server side default if not specified, but want the user to be able
to specify a limit of 0 to request no limit.

_One could just leave it to the user to specify the max value as this would yield the same behaviour without any custom
handling necessary, but let's say you're really set on special casing 0 and it not being the default._

To achieve this we need [explicit presence] tracking for the scalar field. As mentioned above, we could get this by
putting the field inside a oneof.

```protobuf
message MyMessage {
  oneof limit_opt {
    uint64 limit = 1;
  }
}
```

In newer versions of protoc there is also a short-hand for this, with the additional benefit that many code generators
will hide the auto-generated oneof from the generated code.

```protobuf
message MyMessage {
  optional uint64 limit = 1;
}
```

It is perhaps unfortunate that this label is `optional` as to the casual observer this suggests other fields aren't
optional, which isn't the case - all fields in proto 3 are optional, but not all fields are nullable. In earlier drafts
of the RFC this feature was indeed called `nullable`, but at some point that got changed

[proto 3]: https://developers.google.com/protocol-buffers/docs/proto3

[oneof]: https://developers.google.com/protocol-buffers/docs/proto3#oneof

[scalar fields]: https://developers.google.com/protocol-buffers/docs/proto3#scalar

[default value]: https://developers.google.com/protocol-buffers/docs/proto3#default

[design decision]: https://github.com/protocolbuffers/protobuf/issues/1606#issuecomment-281832148

[explicit presence]: https://github.com/protocolbuffers/protobuf/blob/v3.12.0/docs/field_presence.md

## Text Representation

Protobuf is a binary serialization format, which has many advantages but it comes at a price: when
debugging/troubleshooting/exploring a system that contains serialized binary blobs one ends up inventing new swear words
to the tune of ^H<C0><84>=^Z^D^H^B^Z^@!! and yelling them aloud, at the great confusion of their significant other.

### JSON

Protobuf 3 has a defined mapping to [JSON] and this is often the easiest way to interact with protobuf. Unfortunately,
there does not appear to be a tool that takes a proto definition, and a protobuf file, and converts to/from JSON.

That being said tools like [grpcurl] and [grpcui] allow interacting with gRPC services, automatically converting to/from
the human-readable JSON representation. Additionally, the IOx CLI produces, and in future may accept, the JSON
representation of the Protobuf types that IOx uses. This is functionality provided by [pbjson].

[JSON]: https://developers.google.com/protocol-buffers/docs/proto3#json

[grpcurl]: https://github.com/fullstorydev/grpcurl****

[grpcui]: https://github.com/fullstorydev/grpcui

[pbjson]: https://github.com/influxdata/pbjson

### prototxt

The `protoc` tool can (de)serialize to and from binary protobuf files, with the `--decode` and `--encode` flags.
However, it needs to be passed the schema of the files *and* the fully qualified type name of the top level message in
the file to be decoded.

We have a script that hides the boring parts of finding all the `.proto` files and adding the right import paths etc. It
also has a convenient command that lists all the fully qualified message types.

```console
$ ./scripts/prototxt types | grep DatabaseRules
influxdata.iox.management.v1.DatabaseRules
```

You can now decode the binary protobuf into a textual form you can inspect and/or edit:

```console
$ ./scripts/prototxt decode influxdata.iox.management.v1.DatabaseRules \
    < /tmp/iox-data/1/foobar_weather/rules.pb
name: "foobar_weather"
partition_template {
  parts {
    time: "%Y-%m-%d %H:00:00"
  }
}
mutable_buffer_config {
  buffer_size: 1000000
  partition_drop_order {
    order: ORDER_DESC
    created_at_time {
    }
  }
}
```

You can edit the file and then re-encode it back into binary protobuf:

```console
$ ./scripts/prototxt encode influxdata.iox.management.v1.DatabaseRules \
   < /tmp/rules.txt \
   > /tmp/iox-data/1/foobar_weather/rules.pb
$ cat /tmp/iox-data/1/foobar_weather/rules.pb | hexdump -C
00000000  0a 0e 66 6f 6f 62 61 72  5f 77 65 61 74 68 65 72  |..foobar_weather|
00000010  12 15 0a 13 1a 11 25 59  2d 25 6d 2d 25 64 20 25  |......%Y-%m-%d %|
00000020  48 3a 30 30 3a 30 30 3a  0a 08 c0 84 3d 1a 04 08  |H:00:00:....=...|
00000030  02 1a 00                                          |...|
```

### Textpb

The textual protobuf encoding (aka `textpb`) may be unfamiliar to most people. There is no public specification and
implementors just reference the C++ protobuf impl. It's superseded by the `jsonpb` syntax, but you're likely to have to
interact with `textpb` for a while, at least until there is better support for `jsonpb` in commandline tools.

Quick&dirty doc:

1. It's not JSON
2. It's a faithful translation of the original binary message.
3. It uses the schema to map between field tags and field names.
4. In protobuf, the top level is always a message. Textpb is the same.
5. When a field value is an message, use `{}`. (see below of alternative renderings).
6. Repeated fields are not arrays, they are literally repeated fields.

There is a way to visualize a binary protobuf even if you don't know the schema:

```console
$ protoc --decode_raw </tmp/iox-data/1/foobar_weather/rules.pb
1: "foobar_weather"
2 {
  1 {
    3: "%Y-%m-%d %H:00:00"
  }
}
7 {
  1: 1000000
  3 {
    1: 2
    3: ""
  }
}
```

If you compare this carefully with the output of `./scripts/prototxt decode influxdata.iox.management.v1.DatabaseRules`
shown above you'll notice that it has exactly the same data, and only the labels of the fields change.

Thus, if you understand how the protobuf binary encoding lays down their fields, you'll understand `textpb`.

For example, "arrays" are fields with the same tag appearing multiple times in the same message. They don't even need to
appear consecutively.

### Alt-renderings

To complicate matters further, there are alternative renderings of `textpb`. These are all valid:

```
  parts: {
    time: "%Y-%m-%d %H:00:00"
  }
  parts: <
    time: "%Y-%m-%d %H:00:00"
  >
  parts <
    time: "%Y-%m-%d %H:00:00"
  >
```

(echoes of a distant past, when JSON wasn't a thing yet).
