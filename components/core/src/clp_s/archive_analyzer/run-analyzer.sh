#!/usr/bin/env bash

# Runs the CLP archive analyzer in a container and writes a shareable report.
#
# Examples:
#   ./run-analyzer.sh /path/to/archives
#   ./run-analyzer.sh /path/to/archives --no-columns --merge-estimate 16
#   ./run-analyzer.sh s3://my-bucket/archives/ --sample 20
#
# The analyzer only reads the archives you point it at and writes three files to the output
# directory; nothing is sent anywhere. Of those files, only the report is meant to be shared.

set -o errexit
set -o nounset
set -o pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

image="${ARCHIVE_ANALYZER_IMAGE:-archive-analyzer:v0.1}"
image_tarball="${ARCHIVE_ANALYZER_TARBALL:-}"
output_dir="${PWD}/analyzer-output"
archive_location=""
analyzer_args=()

print_usage() {
    cat <<USAGE
Usage: $(basename "${BASH_SOURCE[0]}") [OPTIONS] <ARCHIVES>

  <ARCHIVES>  A directory containing clp-s archives, a single archive, or an
              s3://bucket/prefix location.

Options:
  --output-dir DIR      Where to write the results (default: ./analyzer-output).
  --sample N            Analyze a random sample of N archives.
  --seed S              Seed for reproducible sampling.
  --no-columns          Skip the per-column statistics pass (much faster).
  --merge-estimate N    Include merged-dictionary estimates for packs of N archives.
  --image TAG           Container image to run (default: ${image}).
  --image-tarball FILE  Load the image from FILE if it isn't present locally.
  -h, --help            Print this message.

For s3:// locations, AWS credentials are taken from the standard environment
variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN) or from
~/.aws if present.
USAGE
}

# Resolves a path to an absolute one without requiring realpath (which isn't present on all
# platforms).
absolute_path() {
    local target="${1}"
    if [[ -d "${target}" ]]; then
        (cd "${target}" && pwd)
    else
        local parent
        parent="$(cd "$(dirname "${target}")" && pwd)"
        printf '%s/%s' "${parent}" "$(basename "${target}")"
    fi
}

while [[ $# -gt 0 ]]; do
    case "${1}" in
        -h|--help)
            print_usage
            exit 0
            ;;
        --output-dir|--image|--image-tarball|--sample|--seed|--merge-estimate)
            if [[ $# -lt 2 ]]; then
                echo "Error: ${1} requires a value." >&2
                exit 1
            fi
            case "${1}" in
                --output-dir) output_dir="${2}" ;;
                --image) image="${2}" ;;
                --image-tarball) image_tarball="${2}" ;;
                *) analyzer_args+=("${1}" "${2}") ;;
            esac
            shift 2
            ;;
        --no-columns)
            analyzer_args+=("${1}")
            shift
            ;;
        -*)
            echo "Error: unknown option ${1}" >&2
            print_usage >&2
            exit 1
            ;;
        *)
            if [[ -n "${archive_location}" ]]; then
                echo "Error: only one archive location may be given (got \"${archive_location}\"" \
                     "and \"${1}\")." >&2
                echo "Hint: to analyze many archives, pass the directory that contains them." >&2
                exit 1
            fi
            archive_location="${1}"
            shift
            ;;
    esac
done

if [[ -z "${archive_location}" ]]; then
    echo "Error: no archive location given." >&2
    print_usage >&2
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "Error: docker is required but was not found on PATH." >&2
    exit 1
fi

# Load the image if it isn't already available.
if ! docker image inspect "${image}" >/dev/null 2>&1; then
    if [[ -z "${image_tarball}" ]]; then
        # Fall back to an image tarball shipped alongside this script.
        for candidate in \
            "${script_dir}"/archive-analyzer-*.tar.gz \
            "${script_dir}"/archive-analyzer-*.tar
        do
            if [[ -f "${candidate}" ]]; then
                image_tarball="${candidate}"
                break
            fi
        done
    fi
    if [[ -z "${image_tarball}" ]]; then
        echo "Error: image \"${image}\" isn't available locally and no image tarball was found." >&2
        echo "Load it first (docker load < archive-analyzer-<version>.tar.gz), pass" >&2
        echo "--image-tarball FILE, or build it from source with build-from-source.sh." >&2
        exit 1
    fi
    echo "Loading image from ${image_tarball} ..."
    docker load --input "${image_tarball}"
fi

mkdir -p "${output_dir}"
output_dir="$(absolute_path "${output_dir}")"

# Run as the invoking user so the results aren't owned by root.
docker_args=(--rm --user "$(id -u):$(id -g)" --volume "${output_dir}:/out")

if [[ "${archive_location}" == s3://* || "${archive_location}" == http://* \
      || "${archive_location}" == https://* ]]; then
    container_location="${archive_location}"
    for aws_var in \
        AWS_ACCESS_KEY_ID \
        AWS_SECRET_ACCESS_KEY \
        AWS_SESSION_TOKEN \
        AWS_REGION \
        AWS_DEFAULT_REGION
    do
        if [[ -n "${!aws_var:-}" ]]; then
            docker_args+=(--env "${aws_var}")
        fi
    done
    if [[ -d "${HOME}/.aws" ]]; then
        docker_args+=(
            --volume "${HOME}/.aws:/aws:ro"
            --env "AWS_SHARED_CREDENTIALS_FILE=/aws/credentials"
            --env "AWS_CONFIG_FILE=/aws/config"
        )
    fi
else
    if [[ ! -e "${archive_location}" ]]; then
        echo "Error: no such path \"${archive_location}\"." >&2
        exit 1
    fi
    archive_location="$(absolute_path "${archive_location}")"
    if [[ -d "${archive_location}" ]]; then
        # The archives are mounted read-only: the analyzer never writes to them.
        docker_args+=(--volume "${archive_location}:/archives:ro")
        container_location="/archives"
    else
        docker_args+=(--volume "$(dirname "${archive_location}"):/archives:ro")
        container_location="/archives/$(basename "${archive_location}")"
    fi
fi

echo "Running ${image} on ${archive_location} ..."
docker run \
    "${docker_args[@]}" \
    "${image}" \
    "${container_location}" \
    --output-dir "/out" \
    ${analyzer_args[@]+"${analyzer_args[@]}"}

cat <<SUMMARY

Results in ${output_dir}:
  report.txt               <- shareable: no column names, no log content. Review, then send.
  analysis.json            <- keep local (contains column names)
  column_names.local.txt   <- keep local (maps anonymized column IDs to real names)
SUMMARY
