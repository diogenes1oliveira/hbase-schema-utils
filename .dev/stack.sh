#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<eof
Interacts with the development stack

Usage:
  $SCRIPT [ up | rm | shell | sonar | version ]
eof
}

SCRIPT="$0"
HBASE_TEST_INSTANCE_TYPE="${HBASE_TEST_INSTANCE_TYPE:-local}"
SONAR_USER='admin'
SONAR_PASSWORD="${SONAR_PASSWORD:-password}"
SONAR_PROJECT_NAME="${SONAR_PROJECT_NAME:-hbase-schema-utils}"
SONAR_PROJECT_KEY="${SONAR_PROJECT_KEY:-hbase-schema-utils}"
SONAR_TOKEN="${SONAR_TOKEN:-}"
SONAR_WEB_SECRET="${SONAR_WEB_SECRET:-}"
SONAR_URL="${SONAR_URL:-http://sonar.localhost:9000}"

main() {
  up_to_root
  touch .env
  "$@"
}

setup_env() {
  conf_add_if_absent HBASE_TEST_INSTANCE_TYPE
  conf_replace SONAR_USER
  conf_replace SONAR_PASSWORD
  conf_replace SONAR_PROJECT_NAME
  conf_replace SONAR_PROJECT_KEY
  conf_replace SONAR_TOKEN
}

command_up() {
  touch .env
  (
    cd .dev
    docker-compose --env-file ../.env up --remove-orphans --renew-anon-volumes --detach
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
  conf_remove SONAR_TOKEN
  docker volume prune -f
  docker network prune -f
}

command_shell() {
  hbase_exec hbase shell
}

command_version() {
  log_info "getting version from Maven"
  # shellcheck disable=SC2016
  cmd=(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
  if ! "${cmd[@]}" 2>/dev/null; then
    echo >&2 "ERROR: command failed: ${cmd[*]}"
    return 1
  fi
}

command_sonar() {
  version="$(command_version)"
  mvn org.sonarsource.scanner.maven:sonar-maven-plugin:3.9.1.2184:sonar \
    -Dsonar.login="${SONAR_TOKEN}" \
    -Dsonar.host.url="${SONAR_URL}" \
    -Dsonar.sourceEncoding=UTF-8 \
    -Dsonar.verbose=true \
    -Dsonar.java.binaries=target/classes/* \
    -Dsonar.java.source=1.8 \
    -Dsonar.projectVersion="${version}" \
    -Dsonar.projectKey="${SONAR_PROJECT_KEY}" \
    -Dsonar.projectName="${SONAR_PROJECT_NAME}" \
    -Dsonar.java.coveragePlugin=jacoco \
    -Dsonar.dynamicAnalysis=reuseReports \
    -Dsonar.coverage.jacoco.xmlReportPaths=target/jacoco/jacoco.xml \
    -Dsonar.language=java

  rm -rf .scannerwork/
}

hbase_exec() {
  flags=-i
  if [ -t 0 ]; then
    flags=-it
  fi

  docker exec "${flags}" hbase "$@"
}

hbase_wait_for_shell() {
  wait_for_success hbase 3 hbase_exec hbase-shell-run.sh 'list' >/dev/null
}

sonar_wait_for_port() {
  wait_for_success 'sonar port' 9 nc -z localhost 9000
}

sonar_wait_for_initialization() {
  wait_for_success 'sonar server' 27 curl -s -f -u "${SONAR_USER}:admin" -X POST \
    -w "$(log_info 'HTTP %{http_code}' 2>&1)\\n" \
    "http://localhost:9000/api/users/change_password?login=${SONAR_USER}&previousPassword=admin&password=${SONAR_PASSWORD}" >&2
}

sonar_create_project() {
  log_info "creating sonar project '${SONAR_PROJECT_NAME}'"
  curl -s -f -u "${SONAR_USER}:${SONAR_PASSWORD}" -X POST \
    "http://localhost:9000/api/projects/create?name=${SONAR_PROJECT_NAME}&project=${SONAR_PROJECT_KEY}" >&2 >/dev/null
}

sonar_setup_token() {
  log_info "creating sonar token 'token'"

  url='http://localhost:9000/api/user_tokens/generate'
  cmd=(curl -s -f -u "${SONAR_USER}:${SONAR_PASSWORD}" -X POST -F name='token' "${url}")

  if ! response="$("${cmd[@]}")"; then
    echo >&2 "ERROR: failed to setup 'token' with ${cmd[*]}"
    exit 1
  fi

  SONAR_TOKEN="$(jq -r .token <<<"${response}")"
  log_info "generated token=${SONAR_TOKEN}"
}

conf_is_present() {
  name="$1"
  file="${2:-.env}"
  [ -f "${file}" ] && grep -q "^${name}=" "${file}"
}

conf_add() {
  name="$1"
  file="${2:-.env}"
  value="${!name}"

  if [ -f "${file}" ]; then
    sed -i '$a\' "${file}"
  fi
  echo "${name}=${value}" >>"${file}"
}

conf_remove() {
  name="$1"
  file="${2:-.env}"

  if [ -f "${file}" ]; then
    sed -i "/^${name}=/d" "${file}"
  fi
}

conf_add_if_absent() {
  if ! conf_is_present "$@"; then
    conf_add "$@"
  fi
}

conf_replace() {
  conf_remove "$@"
  conf_add "$@"
}

wait_for_success() {
  name="$1"
  tries="$2"
  shift 2
  i=0

  while true; do
    i="$((i + 1))"
    if [ "$i" -gt "${tries}" ]; then
      echo >&2 "ERROR: ${name} still isn't up after ${tries} retries, giving up"
      return 1
    fi
    log_info "checking if ${name} is up ($i/${tries})..."
    if "$@"; then
      log_info "${name} is up"
      break
    elif ! [ "$i" -eq "${tries}" ]; then
      log_info "${name} still isn't up, will retry in 5 seconds..."
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

log_info() {
  echo "$(date '+%H:%M:%S') [INFO] - $*" >&2
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

# Derives a potential function name from the command.
# For instance, the command 'do-stuff' would call the function 'command_do_stuff'
function_name="command_${command_name/-/_}"

if ! function_exists "${function_name}"; then
  usage >&2
  echo >&2
  echo >&2 "ERROR: unrecognized command '${command_name}'"
  exit 1
fi

main "${function_name}" "$@"
