#!/usr/bin/env bash

# Builds the archive-analyzer container image from source (default), or exports just the binary
# with --binary. Requires Docker. Run from anywhere; the script locates the repository root.

set -o errexit
set -o nounset
set -o pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
repo_root="$(cd "${script_dir}/../../../../.." &>/dev/null && pwd)"

cd "${repo_root}"
git submodule update --init --recursive

if [[ "${1:-}" == "--binary" ]]; then
    docker build \
        --file "${script_dir}/Dockerfile" \
        --target artifact \
        --output "type=local,dest=${script_dir}/out" \
        .
    echo
    echo "Built: ${script_dir}/out/archive-analyzer"
    sha256sum "${script_dir}/out/archive-analyzer"
else
    docker build \
        --file "${script_dir}/Dockerfile" \
        --tag archive-analyzer \
        .
    echo
    echo "Built image: archive-analyzer"
    echo "Example: docker run --rm -v /data/archives:/archives -v \"\$PWD/out:/out\" \\"
    echo "             archive-analyzer /archives/<archive-id> --output-dir /out"
fi
