#!/bin/bash

if [ $# -ne 1 ]; then
	>&2 echo "Usage: $0 REVISION_RANGE

$0 iterates through each commit in REVISION_RANGE and exits 1 if all of these conditions are met:

1. The commit introduces a file that git considers to be a binary file.
2. The commit body does NOT contain a line of matching the pattern 'Adds-Binary: path/to/binary/file'.
   The path must be relative to the repository root.

REVISION_RANGE is typically given as mybranch...master. Note: 3 dots, not 2."
	exit 1
fi

ORIG_IFS="$IFS"
IFS=$'\n' # Set internal field separator to newlines only, so the "for binFile" loop can handle filenames containing spaces.
BINARY_FILES_INTRODUCED=0
for rev in $(git rev-list "$1"); do
	# This loop is a bit complicated, so here is the explanation:
	# First, use git log on the single revision.
	# We can't use --numstat because that doesn't differentiate between binary files added and removed.
	# Grep for lines indicating a Binary file went from zero to non-zero bytes.
	# Then cut down to just the entire field before the pipe. This will break if we ever have a binary file whose name contains the pipe character.
	# Finally, print just the first field without leading or trailing spaces. (https://unix.stackexchange.com/a/205854)
	for binFile in $(git log -1 --format='' --stat=255 "$rev" | grep ' Bin 0 ->' | cut -d '|' -f 1 | awk '{$1=$1;print}'); do
		# We have found a new binary file in $rev.
		# Was it in the commit's whitelist?
		# (GitHub seems to use \r\n on Squash&Merge commit messages, which doesn't play well with grep -x; hence the awk line.)
		if git log -1 --format=%b "$rev" | awk '{ sub("\r$", ""); print }' | grep -q -F -x "Adds-Binary: $binFile"; then
			# Yes it was. Skip this file.
			echo "Revision $rev $(git log -1 --format='[%s]' "$rev") added whitelisted binary file: $binFile"
			continue
		fi

		echo "Revision $rev $(git log -1 --format='[%s]' "$rev") introduced binary file: $binFile"
		BINARY_FILES_INTRODUCED=1
	done
done
IFS="$ORIG_IFS"

if [ $BINARY_FILES_INTRODUCED -eq 1 ]; then
	echo
	echo '--------------------------------------------------'
	echo "This changeset introduced unexpected binary files.

If you meant to include them, amend the commit(s) that introduced the file(s),
to include a line that matches exactly 'Adds-Binary: path/to/binary/file'.

If you did not mean to include the file, please add an appropriate line to .gitignore
so that other developers do not mistakenly commit that file, and please amend your commit
to remove the file."
	exit 1
fi
