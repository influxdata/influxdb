# InfluxDB Binary Protocol (Experimental)

* provide fast insert api.
* provide expandable, easy to implement protocol
* provide stateful protocol. (don't need to send auth info every time)
* reduce network traffic

This binary protocol is as is (not perfect). but it works fine.

## Supported transfer protocol

* TCP v4 (also supports TLS)
* Unix Domain Socket

## Protocol Basics

```
|4bytes length | protobuf payload |
```

## Handshake Flow


```
    +-------------+
    | STARTUP MSG |
    +------+------+
           |
           |
    +------v------+   +-------------+
    | STARTUP RSP +---> UPGRADE SSL |
    +------+------+   +------+------+
           |                 |
           |                 |
    +------v------+   +------v------+
    |   AUTH MSG  <---+  HANDSHAKE  |
    +------+------+   +-------------+
           |
           |
    +------v------+   +-------------+
    |   AUTH OK   +--->  OPTION MSG |
    +------+------+   +-----+-------+
           |                |
           |                |
    +------v------+         |
    |    READY    <---------+
    +-------------+
``