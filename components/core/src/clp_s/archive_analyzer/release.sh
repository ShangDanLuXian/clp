#!/usr/bin/env bash

# Builds the archive-analyzer image and packages it as a distributable tarball, ready to send to
# a customer (who then runs `docker load` - no build tooling needed on their side).

set -o errexit
set -o nounset
set -o pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
repo_root="$(cd "${script_dir}/../../../../.." &>/dev/null && pwd)"

version="$(git -C "${repo_root}" describe --always --dirty)"
image_tag="archive-analyzer:${version}"

"${script_dir}/build.sh"
docker tag archive-analyzer "${image_tag}"

out_dir="${script_dir}/out"
mkdir -p "${out_dir}"
tarball="${out_dir}/archive-analyzer-${version}.tar.gz"
docker save "${image_tag}" | gzip > "${tarball}"

echo
echo "Release tarball: ${tarball}"
sha256sum "${tarball}"
echo
echo "Customer instructions:"
echo "  docker load < $(basename "${tarball}")"
echo "  docker run --rm -v /data/archives:/archives -v \"\$PWD/out:/out\" \\"
echo "      ${image_tag} /archives/<archive-id> --output-dir /out"
