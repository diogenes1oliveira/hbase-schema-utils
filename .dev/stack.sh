#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<eof
Interacts with the development stack

Usage:
  $SCRIPT [ start | stop ]
eof
}

COMMAND=""

main() {
  up_to_root
  "${COMMAND}"
}

stack_start() {
  cd .dev
}

stack_stop() {
  cd .dev
}

SCRIPT="$0"

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
-h | --help | help)
  usage
  exit 0
  ;;
start | stop )
  COMMAND="stack_$1"
  ;;
*)
  usage >&2
  echo >&2
  echo >&2 "ERROR: unrecognized command '${1:-}'"
  exit 1
  ;;
esac

main "$@"
