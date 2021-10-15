//go:build tools
// +build tools

package main

// todo(dstrand1): remove gogo generators when influx_tools is ported
import (
	_ "github.com/benbjohnson/tmpl"
	_ "github.com/gogo/protobuf/protoc-gen-gogo"
	_ "github.com/gogo/protobuf/protoc-gen-gogofaster"
	_ "github.com/influxdata/pkg-config"
	_ "golang.org/x/tools/cmd/goimports"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
