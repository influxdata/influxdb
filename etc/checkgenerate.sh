#!/bin/bash

set -e

function check_changes () {
  changes="$(git status --porcelain=v1 2>/dev/null)"
  if [ -n "$changes" ] ; then
    echo $1
    echo "$changes"
    exit 1
  fi
}

check_changes "git is dirty before running 'make generate-sources!'"
make generate-sources
check_changes "git is dirty after running 'make generate-sources'!"
