#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<eof
Interacts with the development stack

Usage:
  $SCRIPT [ up | rm | shell ]
eof
}

SCRIPT="$0"

main() {
  function_name="command_${1/-/_}"
  shift

  up_to_root
  setup_env
  "${function_name}" "$@"
}

setup_env() {
  touch .env
  if ! grep -q HBASE_TEST_INSTANCE_TYPE .env; then
    {
      echo
      echo "# set by .dev/stack.sh on $(date -u --iso=seconds)"
      echo HBASE_TEST_INSTANCE_TYPE=local
    } >> .env
  fi
}

command_up() {
  cd .dev
  docker-compose up --remove-orphans --renew-anon-volumes --detach
  hbase_wait_for_shell
}

command_rm() {
  cd .dev
  docker-compose kill -s 9
  docker-compose rm -fsv
  docker volume prune -f
  docker network prune -f
}

command_shell() {
  hbase_exec hbase shell
}

hbase_exec() {
  flags=-i
  if [ -t 0 ]; then
    flags=-it
  fi

  docker exec "${flags}" hbase "$@"
}

hbase_wait_for_shell() {
  i=0

  while true; do
    i="$((i+1))"
    if [ "$i" -gt 5 ]; then
      echo >&2 "ERROR: hbase is still not up after 5 retries, giving up"
      return 1
    fi
    echo >&2 "INFO: checking if hbase is up ($i/5)..."
    if hbase_exec hbase-shell-run.sh 'list' > /dev/null; then
      echo >&2 "INFO: hbase is up"
      break
    elif ! [ "$i" -eq 5 ]; then
      echo >&2 "INFO: hbase is still not up, will retry in 1 second..."
      sleep 1
    fi
  done
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
-h | --help | help)
  usage
  exit 0
  ;;
up | rm | shell)
  # just to check
  ;;
*)
  usage >&2
  echo >&2
  echo >&2 "ERROR: unrecognized command '${1:-}'"
  exit 1
  ;;
esac

main "$@"
