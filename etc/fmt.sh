#!/bin/bash

go install golang.org/x/tools/cmd/goimports

# For every Go file in the project, excluding vendor...
for file in $(go list -f '{{$dir := .Dir}}{{range .GoFiles}}{{printf "%s/%s\n" $dir .}}{{end}}{{range .TestGoFiles}}{{printf "%s/%s\n" $dir .}}{{end}}{{range .IgnoredGoFiles}}{{printf "%s/%s\n" $dir .}}{{end}}{{range .CgoFiles}}{{printf "%s/%s\n" $dir .}}{{end}}' ./... ); do
  # ... if file does not contain standard generated code comment (https://golang.org/s/generatedcode)...
  if ! grep -Exq '^// Code generated .* DO NOT EDIT\.$' $file; then
    gofmt -w -s $file
    goimports -w $file
  fi
done
