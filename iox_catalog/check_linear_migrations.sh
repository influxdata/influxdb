#!/usr/bin/env bash
#
# This script checks that migrations are modified correctly.

set -euo pipefail

# arg parsing
if [ "$#" -ne 1 ]; then
    echo "ERROR: need to give a path"
    exit 1
fi
path="$1"

# go to correct path
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR"

# check branch
main_branch="main"
current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" == "$main_branch" ]]; then
    echo "INFO:  do not run on '$main_branch'"
    exit 0
else
    echo "INFO:  compare to branch: '$main_branch'"
fi

# check new versions
versions_main="$(git ls-tree -r --name-only "$main_branch" "$path" | xargs -n1 basename | sed -E 's:^([0-9]+).*:\1:g' | sort -n)"
version_last_main="$(echo "$versions_main" | tail -1)"
echo "INFO:  last existing version: $version_last_main"

versions_new="$(git diff --name-only --diff-filter=A --no-renames "$main_branch" HEAD -- "$path" | xargs -rn1 basename | sed -E 's:^([0-9]+).*:\1:g' | sort -n)"
version_first_new="$(echo "$versions_new" | head -1)"
echo "INFO:  first new version:     $version_first_new"

if [[ -n "$version_first_new" && "$version_first_new" < "$version_last_main" ]]; then
    echo "ERROR: new version BEFORE existing ones"
    exit 1
else
    echo "INFO:  new versions are AFTER existing ones"
fi

# check deleted versions
versions_deleted="$(git diff --name-only --diff-filter=D --no-renames "$main_branch" HEAD -- "$path" | xargs -rn1 basename | sed -E 's:^([0-9]+).*:\1:g' | sort -n)"

if [[ -n "$versions_deleted" ]]; then
    echo "ERROR: deleted versions:"
    echo "$versions_deleted"
    exit 1
else
    echo "INFO:  no deleted versions"
fi

# check edited versions
files_modified="$(git diff --name-only --diff-filter=M --no-renames "$main_branch" HEAD -- "$path" | sort)"
if [[ -n "$files_modified" ]]; then
    readarray -t files_modified <<<"$files_modified"

    for f in "${files_modified[@]}"; do
        version="$(basename "$f" | sed -E 's:^([0-9]+).*:\1:g')"
        checksum_old="$(git show "$main_branch:$f" | sha384sum | sed -E 's:^([0-9a-f]+).*:\1:g')"
        pragma="-- IOX_OTHER_CHECKSUM: $checksum_old"
        if grep -qF -- "$pragma" "../$f"; then
            echo "INFO:  version $version modified correctly"
        else
            echo "ERROR: modified version $version is missing correct pragma: '$pragma'"
            exit 1
        fi
    done
else
    echo "INFO:  no modified versions"
fi

# perfect
echo "INFO:  all good"
