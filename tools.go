// +build tools

package main

import (
	_ "github.com/benbjohnson/tmpl"
	_ "github.com/gogo/protobuf/protoc-gen-gogo"
	_ "github.com/gogo/protobuf/protoc-gen-gogofaster"
	_ "github.com/influxdata/pkg-config"
	_ "golang.org/x/tools/cmd/goimports"
)
