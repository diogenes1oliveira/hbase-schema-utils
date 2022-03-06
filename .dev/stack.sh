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
SONAR_USER='admin'
SONAR_PASSWORD="${SONAR_PASSWORD:-password}"
SONAR_PROJECT="${SONAR_PROJECT:-project}"
SONAR_TOKEN="${SONAR_TOKEN:-}"

main() {
  up_to_root
  touch .env
  "$@"
}

setup_env() {
  env_add_if_absent HBASE_TEST_INSTANCE_TYPE local
  env_replace SONAR_USER "${SONAR_USER}"
  env_replace SONAR_PASSWORD "${SONAR_PASSWORD}"
  env_replace SONAR_PROJECT "${SONAR_PROJECT}"
  env_replace SONAR_TOKEN "${SONAR_TOKEN}"
}

command_up() {
  (
    cd .dev
    docker-compose up --remove-orphans --renew-anon-volumes --detach
  )
  hbase_wait_for_shell
  sonar_wait_for_port
  sonar_wait_for_initialization
  sonar_create_project

  sonar_setup_token
  setup_env
}

command_rm() {
  (
    cd .dev
    docker-compose kill -s 9
    docker-compose rm -fsv
  )
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
  wait_for_success hbase 5 hbase_exec hbase-shell-run.sh 'list' >/dev/null
}

sonar_wait_for_port() {
  wait_for_success 'sonar port' 10 nc -z localhost 9000
}

sonar_wait_for_initialization() {
  wait_for_success 'sonar server' 30 curl -s -f -u "${SONAR_USER}:admin" -X POST -w 'INFO: HTTP %{http_code}\n' \
    "http://localhost:9000/api/users/change_password?login=${SONAR_USER}&previousPassword=admin&password=${SONAR_PASSWORD}" >&2
}

sonar_create_project() {
  echo >&2 "INFO: creating sonar project '${SONAR_PROJECT}'"
  curl -s -f -u "${SONAR_USER}:${SONAR_PASSWORD}" -X POST \
    "http://localhost:9000/api/projects/create?name=${SONAR_PROJECT}&project=${SONAR_PROJECT}" >&2 > /dev/null
}

sonar_setup_token() {
  echo >&2 "INFO: creating sonar token 'token'"

  url='http://localhost:9000/api/user_tokens/generate'
  cmd=( curl -s -f -u "${SONAR_USER}:${SONAR_PASSWORD}" -X POST -F name=token "${url}" )

  if ! response="$( "${cmd[@]}" )"; then
    echo >&2 "ERROR: failed to setup token with ${cmd[*]}"
    exit 1
  fi

  SONAR_TOKEN="$( jq -r .token <<<"${response}" )"
  echo >&2 "INFO: Generated token ${SONAR_TOKEN}"
}

command_add() {
  env_add_if_absent "$@"
}

env_add_if_absent() {
  name="$1"
  value="$2"

  if ! grep -q "^${name}=" .env; then
    sed -i '$a\' .env
    echo "${name}=${value}" >>.env
  fi
}

env_replace() {
  name="$1"
  value="$2"

  sed -i "/^${name}=/d" .env
    sed -i '$a\' .env
  echo "${name}=${value}" >>.env
}

wait_for_success() {
  name="$1"
  tries="$2"
  shift 2
  i=0

  while true; do
    i="$((i + 1))"
    if [ "$i" -gt "${tries}" ]; then
      echo >&2 "ERROR: ${name} is still not up after ${tries} retries, giving up"
      return 1
    fi
    echo >&2 "INFO: checking if ${name} is up ($i/${tries})..."
    if "$@"; then
      echo >&2 "INFO: ${name} is up"
      break
    elif ! [ "$i" -eq "${tries}" ]; then
      echo >&2 "INFO: ${name} is still not up, will retry in 5 seconds..."
      sleep 5
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

function_exists() {
  function_name="$1"
  declare -f -F "${function_name}" >/dev/null
}

case "${1:-}" in
-h | --help | help)
  usage
  exit 0
  ;;
*) ;;

esac

command_name="${1:-}"
shift
function_name="command_${command_name/-/_}"

if ! function_exists "${function_name}"; then
  usage >&2
  echo >&2
  echo >&2 "ERROR: unrecognized command '${command_name}'"
  exit 1
fi

main "${function_name}" "$@"
