# example usage

Run server:

```console
cargo run -p grpc-binary-logger --example server
```

Run client:

```
cargo run -p grpc-binary-logger --example client
```

View binary log:

```console
$ go install mkm.pub/binlog
$ binlog stats /tmp/grpcgo_binarylog.bin
Method			[≥0s]	[≥0.05s][≥0.1s]	[≥0.2s]	[≥0.5s]	[≥1s]	[≥10s]	[≥100s]	[errors]
/test.Test/TestUnary	1	0	0	0	0	0	0	0
$ binlog debug /tmp/grpcgo_binarylog.bin
1	EVENT_TYPE_CLIENT_HEADER	/test.Test/TestUnary
1	EVENT_TYPE_CLIENT_MESSAGE
1	EVENT_TYPE_SERVER_HEADER
1	EVENT_TYPE_SERVER_MESSAGE
1	EVENT_TYPE_SERVER_TRAILER
$ binlog view /tmp/grpcgo_binarylog.bin
ID	When				Elapsed	Method			Status
1	2022/08/24 14:33:08.736308	1.217ms	/test.Test/TestUnary	OK
