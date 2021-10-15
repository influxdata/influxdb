#!/bin/bash -e

go install google.golang.org/protobuf/cmd/protoc-gen-go
go install golang.org/x/tools/cmd/stringer
go install github.com/benbjohnson/tmpl


function check_changes () {
  changes="$(git status --porcelain=v1 2>/dev/null)"
  if [ -n "$changes" ] ; then
    echo $1
    echo "$changes"
    exit 1
  fi
}

check_changes "git is dirty before running generate!"

go generate ./...

check_changes "git is dirty after running generate!"
