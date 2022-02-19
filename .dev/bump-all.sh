#!/usr/bin/env bash

set -euo pipefail

SCRIPT="$0"

usage() {
  cat <<eof
Bumps all versions in the root pom.xml and in all submodules

Usage:
  $SCRIPT NEW_VERSION
eof
}

main() {
  up_to_root
  mvn versions:set -DnewVersion="$1" -DgenerateBackupPoms=false
}

up_to_root() {
  # Directory containing this script
  cd "$(dirname "$(absolute_path "$SCRIPT")")"

  cd "$(git rev-parse --show-toplevel)"
}

# Taken from https://stackoverflow.com/a/20500246, supposedly more portable
absolute_path() (
    cd "$(dirname "$1")"
    printf "%s/%s" "$(pwd)" "$(basename "$1")"
)

case "${1:-}" in
-h | --help | help )
  usage
  ;;
'')
  usage >&2
  echo >&2
  echo >&2 "ERROR: no new version"
  exit 1
  ;;
esac

main "$@"
