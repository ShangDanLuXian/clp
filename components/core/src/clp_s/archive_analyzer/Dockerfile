# Builds the archive-analyzer binary from source in a clean container, so the binary you run is
# one you produced yourself from source you can read.
#
# Build from the ROOT of the repository (see build.sh, which wraps this):
#
#   docker build \
#       --file components/core/src/clp_s/archive_analyzer/Dockerfile \
#       --target artifact \
#       --output type=local,dest=components/core/src/clp_s/archive_analyzer/out \
#       .
#
# The binary is written to components/core/src/clp_s/archive_analyzer/out/archive-analyzer.
#
# NOTE: The build disables CLP's optional libcurl support (CLP_BUILD_CLP_S_ENABLE_CURL=OFF), so no
# networking library is linked into the binary at all — verifiable with ldd.

FROM ubuntu:22.04 AS build

ARG DEBIAN_FRONTEND=noninteractive

# Install basic tooling, Java (required by the ANTLR parser generation step), and CLP's build
# dependencies using the repository's own install scripts.
COPY components/core/tools/scripts/lib_install /tmp/lib_install
RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends \
        ca-certificates \
        curl \
        default-jre-headless \
        git \
    && /tmp/lib_install/ubuntu-jammy/install-all.sh \
    && rm -rf /var/lib/apt/lists/*

# Install Go Task (the build orchestrator used by CLP).
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin

WORKDIR /clp
COPY . .

# Download and build CLP's third-party dependencies (checksum-pinned in taskfiles/deps/main.yaml),
# and generate the KQL/SQL parsers.
RUN task deps:core \
    && task codegen:clp-s-generate-parsers

# Configure and build only the archive-analyzer target, with networking support disabled.
RUN cmake -S components/core -B build/core \
        -C build/deps/cpp/cmake-settings/all-core.cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DCLP_BUILD_CLP_S_ENABLE_CURL=OFF \
        -DCLP_BUILD_TESTING=OFF \
    && cmake --build build/core --target archive-analyzer --parallel

FROM scratch AS artifact
COPY --from=build /clp/build/core/archive-analyzer /archive-analyzer
