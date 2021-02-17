#!/bin/bash

go install golang.org/x/tools/cmd/goimports

HAS_FMT_ERR=0
# For every Go file in the project, excluding vendor...

for file in $(go list -f '{{$dir := .Dir}}{{range .GoFiles}}{{printf "%s/%s\n" $dir .}}{{end}}{{range .TestGoFiles}}{{printf "%s/%s\n" $dir .}}{{end}}{{range .IgnoredGoFiles}}{{printf "%s/%s\n" $dir .}}{{end}}{{range .CgoFiles}}{{printf "%s/%s\n" $dir .}}{{end}}' ./... ); do
  # ... if file does not contain standard generated code comment (https://golang.org/s/generatedcode)...
  if ! grep -Exq '^// Code generated .* DO NOT EDIT\.$' $file; then
    FMT_OUT="$(goimports -l -d $file)"
    # ... and if goimports had any output...
    if [[ -n "$FMT_OUT" ]]; then
      if [ "$HAS_FMT_ERR" -eq "0" ]; then
        # Only print this once.
        HAS_FMT_ERR=1
        echo 'Commit includes files that are not gofmt-ed' && \
        echo 'run "make fmt"' && \
        echo ''
      fi
      echo "$FMT_OUT" # Print output and continue, so developers don't fix one file at a t
    fi
   fi
done

## print at the end too... sometimes it is nice to see what to do at the end.
if [ "$HAS_FMT_ERR" -eq "1" ]; then
    echo 'Commit includes files that are not gofmt-ed' && \
    echo 'run "make fmt"' && \
    echo ''
fi
exit "$HAS_FMT_ERR"
