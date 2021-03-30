# Protobuf

[Protocol Buffers](https://github.com/protocolbuffers/protobuf) (a.k.a., protobuf)
are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data.

Protobuf is a binary serialization format, which has many advantages but it comes at a price:
when debugging/troubleshooting/exploring a system that contains serialized binary blobs one
ends up inventing new swear words at the tune of ^H<C0><84>=^Z^D^H^B^Z^@!! and yelling them aloud,
at the great confusion of the their significant other.

Tools always come to the rescue, but public protobuf tooling is embarrassingly behind.

## prototxt

The `protoc` tool can (de)serialize to and from binary protobuf files,
with the `--decode` and `--encode` flags. However, it needs to be passed the schema of the files *and*
the fully qualified type name of the top level message in the file to be decoded.

We have a script that hides the boring parts of finding all the `.proto` files and adding the right
import paths etc. It also has a convenient command that lists all the fully qualified message types.

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

## Textpb

The textual protobuf encoding (aka `textpb`) may be unfamiliar to most people.
There is no public specification and implementors just reference the C++ protobuf impl.
It's superseded by the `jsonpb` syntax, but you're likely to have to interact with `textpb` for a while,
at least until there is better support for `jsonpb` in commandline tools.

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

If you compare this carefully with the output of `./scripts/prototxt decode influxdata.iox.management.v1.DatabaseRules` shown above
you'll notice that it has exactly the same data, and only the labels of the fields change.

Thus, if you understand how the protobuf binary encoding lays down their fields, you'll understand `textpb`.

For example, "arrays" are fields with the same tag appearing multiple times in the same message.
They don't even need to appear consecutively.

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
