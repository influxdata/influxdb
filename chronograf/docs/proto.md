Download the protobuf binary by either:
- `brew install protobuf`
- Download from protobuf [github release](https://github.com/google/protobuf/releases/tag/v3.1.0) and place in your $PATH


run the following 4 commands listed here https://github.com/gogo/protobuf
```sh
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/jsonpb
go get github.com/gogo/protobuf/protoc-gen-gogo
go get github.com/gogo/protobuf/gogoproto
```

now, you can regenerate the `.proto` file: `protoc --gogo_out=. internal.proto`
