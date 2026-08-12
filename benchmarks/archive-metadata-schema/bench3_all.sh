#!/usr/bin/env bash
# Round 3, script 3 of 3: setup + build + the full query/write matrix, in one command.
# Build flags (--rate, --days, --design) go to setup; query then runs the full matrix.
set -euo pipefail
cd "$(dirname "$0")"
./bench3_setup.sh "$@"
./bench3_query.sh
