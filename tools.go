// +build tools

package main

import (
	_ "github.com/benbjohnson/tmpl"
	_ "github.com/influxdata/pkg-config"
	_ "golang.org/x/tools/cmd/goimports"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
