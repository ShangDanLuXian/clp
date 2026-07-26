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

# Empty means "auto-detect": use an analyzer image that's already loaded, otherwise load one from
# a tarball shipped alongside this script and use whatever tag it provides.
image="${ARCHIVE_ANALYZER_IMAGE:-}"
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
  --max-archives N      Never consider more than N archives (default: 1024). For s3://
                        locations, listing stops at N so a large bucket isn't downloaded
                        in full.
  --no-columns          Skip the per-column statistics pass (much faster).
  --merge-estimate N    Pack size for the merged-dictionary estimate (default: 128).
  --no-merge-estimate   Omit the merged-dictionary estimate.
  --sections LIST       Only include these report sections (failures, similarity, merge,
                        summary, components, dictionaries, columns); default is all.
  --show-column-names   Put real column names in the report instead of anonymized IDs.
                        The report is then NOT safe to share.
  --image TAG           Container image to run (default: auto-detected).
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
        --output-dir|--image|--image-tarball|--sample|--seed|--merge-estimate|--sections\
            |--max-archives)
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
        --no-columns|--show-column-names|--no-merge-estimate)
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

# Resolve which image to run: an explicitly requested one, an already-loaded analyzer image, or
# one loaded from a tarball shipped alongside this script.
if [[ -z "${image}" ]]; then
    for candidate in "archive-analyzer:latest" "archive-analyzer"; do
        if docker image inspect "${candidate}" >/dev/null 2>&1; then
            image="${candidate}"
            break
        fi
    done
fi

if [[ -z "${image}" ]] || ! docker image inspect "${image}" >/dev/null 2>&1; then
    if [[ -z "${image_tarball}" ]]; then
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
        echo "Error: no analyzer image is available locally and no image tarball was found." >&2
        echo "Load one (docker load < archive-analyzer-<version>.tar.gz), pass --image-tarball" >&2
        echo "FILE, or build one from source with build-from-source.sh." >&2
        exit 1
    fi

    # A tarball named archive-analyzer-<version>.tar[.gz] is expected to provide
    # archive-analyzer:<version>; if that image is already loaded, skip loading it again.
    if [[ -z "${image}" ]]; then
        tarball_name="$(basename "${image_tarball}")"
        tarball_name="${tarball_name%.gz}"
        tarball_name="${tarball_name%.tar}"
        expected_image="archive-analyzer:${tarball_name#archive-analyzer-}"
        if docker image inspect "${expected_image}" >/dev/null 2>&1; then
            image="${expected_image}"
        fi
    fi
fi

if [[ -z "${image}" ]] || ! docker image inspect "${image}" >/dev/null 2>&1; then
    echo "Loading image from ${image_tarball} ..."
    load_output="$(docker load --input "${image_tarball}")"
    echo "${load_output}"
    loaded_image="$(printf '%s\n' "${load_output}" | sed -n 's/^Loaded image: //p' | head -n 1)"
    if [[ -z "${image}" ]]; then
        if [[ -z "${loaded_image}" ]]; then
            echo "Error: couldn't determine which image the tarball provides; pass --image TAG." >&2
            exit 1
        fi
        image="${loaded_image}"
    elif ! docker image inspect "${image}" >/dev/null 2>&1; then
        echo "Error: the tarball provides \"${loaded_image}\", not the requested" \
             "\"${image}\"." >&2
        echo "Re-run with --image ${loaded_image} (or omit --image to use it automatically)." >&2
        exit 1
    fi
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
