#!/bin/bash -ex


dependencies="cargo git git-cliff"

for dependency in $dependencies
do
  if ! command -v $dependency &>/dev/null
  then
    echo "error: $dependency was not found in PATH" >&2
    exit 255
  fi
done

# The default "starting" commit is a somewhat arbitrary starting point for
# cataloging recent commits in a way that breaks from the old convention
DEFAULT_START_COMMIT="891e7d47827e99947e46c82f509e479aa13acf24"
DEFAULT_NEWEST_COMMIT="$(git rev-parse HEAD)"
DEFAULT_COMMIT_RANGE="${DEFAULT_START_COMMIT}..${DEFAULT_NEWEST_COMMIT}"

COMMIT_RANGE="${DEFAULT_COMMIT_RANGE}"

DEFAULT_GIT_CLIFF_OPTIONS=""
PREPEND_TARGET=""

function print-usage {
  cat << EOF >&2
usage: $0 [<options>] -- [<git cliff flags>] [<git cliff options>]

    --commit-range <git commit range>       The specific range of commits from which to generate the changelog.
                                            A hardcoded default sets a range that is contemporaneous with the
                                            addition of this script to influxdb CI.
                                            Value: $COMMIT_RANGE

    --prepend <file path>                   Target a file to which to prepend the git-cliff output. This is not
                                            the same as the git-cliff prepend option, which can only be used with
                                            the -l or -u flags in that tool.
                                            Value: $PREPEND_TARGET


Options specified after '--' separator are used by git-cliff directly
EOF
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --commit-range)
      COMMIT_RANGE="$2"
      shift
      ;;
    --prepend)
      PREPEND_TARGET="$2"
      shift
      ;;
    --help)
      print-usage
      exit 255
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "error: unknown option '$1'" >&2
      exit 255
      ;;
  esac
  shift
done

output="$(git cliff ${@} ${DEFAULT_GIT_CLIFF_OPTIONS} ${COMMIT_RANGE})"

if [ -n "$PREPEND_TARGET" ] && [ -n "$output" ]; then
  newline=$'\n\n'
  echo "${output}${newline}$(cat $PREPEND_TARGET)" > $PREPEND_TARGET
else
  echo "$output"
fi
