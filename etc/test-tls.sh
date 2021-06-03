#!/bin/bash
set -eu -o pipefail
readonly GO=${GO:-go}

generate_keypair() {
  local cert key
  cert="$1"
  key="$2"

  openssl req -batch -new \
    -newkey rsa:4096 \
    -x509 \
    -sha256 \
    -days 1 \
    -nodes \
    -subj "/C=US/ST=CA/L=./O=./OU=./CN=." \
    -out "${cert}" \
    -keyout "${key}"
}

run_tls_tests() {
  local cert key
  cert="$1"
  key="$2"

  1>&2 echo Running go tests...
  INFLUXDB_TEST_SSL_CERT_PATH="${cert}" INFLUXDB_TEST_SSL_KEY_PATH="${key}" "${GO}" test -mod=readonly ./cmd/influxd/launcher/_tlstests
}

main() {
  local cert key
  cert=$(pwd)/test.crt
  key=$(pwd)/test.key

  trap "rm -f '${cert}' '${key}'" EXIT
  generate_keypair "${cert}" "${key}"
  run_tls_tests "${cert}" "${key}"
}
main
