file="`tempfile`"
trap "rm \"$file\"" EXIT
go list std > "$file"
go list -f '{{join .Deps "\n"}}' ./... | grep -vxFf "$file" | sort -u
