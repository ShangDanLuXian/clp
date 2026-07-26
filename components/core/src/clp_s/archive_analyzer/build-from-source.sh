#!/usr/bin/env bash

# Builds the archive-analyzer container image from source, at a pinned commit, so you can review
# exactly what you are going to run instead of trusting a prebuilt image.
#
# Usage:
#   ./build-from-source.sh [WORK_DIR]
#
# WORK_DIR defaults to ./archive-analyzer-src. The checkout is left in place for review.
#
# Requirements: docker, git, ~10 GB of free disk. The first build takes 15-30 minutes because it
# compiles CLP's third-party dependencies (which are pinned by checksum) from source.

set -o errexit
set -o nounset
set -o pipefail

# The source this image is built from. Override with the environment variables if you were given
# a different location or version.
repo_url="${ANALYZER_REPO_URL:-https://github.com/ShangDanLuXian/clp.git}"
ref="${ANALYZER_REF:-archive-analyzer-v0.1}"

work_dir="${1:-${PWD}/archive-analyzer-src}"

for required in docker git; do
    if ! command -v "${required}" >/dev/null 2>&1; then
        echo "Error: ${required} is required but was not found on PATH." >&2
        exit 1
    fi
done

if [[ -d "${work_dir}/.git" ]]; then
    echo "Updating existing checkout in ${work_dir} ..."
    git -C "${work_dir}" fetch --tags origin
else
    echo "Cloning ${repo_url} into ${work_dir} ..."
    git clone "${repo_url}" "${work_dir}"
fi

echo "Checking out ${ref} ..."
git -C "${work_dir}" -c advice.detachedHead=false checkout --force "${ref}"
git -C "${work_dir}" submodule update --init --recursive

commit="$(git -C "${work_dir}" rev-parse HEAD)"
short_commit="$(git -C "${work_dir}" rev-parse --short HEAD)"
image_tag="archive-analyzer:${short_commit}-selfbuilt"

analyzer_dir="${work_dir}/components/core/src/clp_s/archive_analyzer"
if [[ ! -x "${analyzer_dir}/build.sh" ]]; then
    echo "Error: ${analyzer_dir}/build.sh not found; is ${ref} the right version?" >&2
    exit 1
fi

echo "Building the image from source (this takes a while on the first run) ..."
"${analyzer_dir}/build.sh"
docker tag "archive-analyzer" "${image_tag}"

cat <<SUMMARY

Built ${image_tag}
  from ${repo_url}
  at   ${ref} (${commit})

The source is checked out in ${work_dir} for review. The analyzer is confined to:
  ${analyzer_dir}
plus a single 'add_subdirectory' line in components/core/src/clp_s/CMakeLists.txt; everything
else is unmodified open-source CLP. To see the whole change:

  git -C "${work_dir}" diff main...${ref} -- components/core/src/clp_s

To run the image you just built:

  ${analyzer_dir}/run-analyzer.sh --image ${image_tag} /path/to/archives

NOTE: Container images are not bit-for-bit reproducible, so this image's digest will not match a
prebuilt one even when built from identical source. The point of this script is that you can run
an image you built yourself from source you can read.
SUMMARY
