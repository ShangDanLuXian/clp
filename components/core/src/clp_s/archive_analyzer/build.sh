#!/usr/bin/env bash

# Builds the archive-analyzer binary from source inside a container and copies it to ./out/.
# Requires Docker. Run from anywhere; the script locates the repository root itself.

set -o errexit
set -o nounset
set -o pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
repo_root="$(cd "${script_dir}/../../../../.." &>/dev/null && pwd)"

cd "${repo_root}"
git submodule update --init --recursive

docker build \
    --file "${script_dir}/Dockerfile" \
    --target artifact \
    --output "type=local,dest=${script_dir}/out" \
    .

echo
echo "Built: ${script_dir}/out/archive-analyzer"
sha256sum "${script_dir}/out/archive-analyzer"
