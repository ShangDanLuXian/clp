# Builds the archive-analyzer from source and packages it as a runnable container image, so the
# image you run is one you (or we) produced from source you can read.
#
# Build from the ROOT of the repository (see build.sh, which wraps this):
#
#   docker build \
#       --file components/core/src/clp_s/archive_analyzer/Dockerfile \
#       --tag archive-analyzer \
#       .
#
# Run (local archives):
#   docker run --rm -v /data/archives:/archives -v "$PWD/out:/out" archive-analyzer \
#       /archives/<archive-id> --output-dir /out
#
# Run (sample of archives on S3; credentials via the standard AWS environment variables):
#   docker run --rm -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
#       -v "$PWD/out:/out" archive-analyzer \
#       s3://bucket/prefix/ --sample 20 --output-dir /out
#
# NOTE: The binary includes libcurl so it can read archives directly from S3. It only ever
# connects to the archive locations passed on the command line - there is no telemetry.
#
# To export just the binary instead of an image:
#   docker build ... --target artifact --output type=local,dest=out .

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

# Configure and build only the archive-analyzer target.
RUN cmake -S components/core -B build/core \
        -C build/deps/cpp/cmake-settings/all-core.cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DCLP_BUILD_TESTING=OFF \
    && cmake --build build/core --target archive-analyzer --parallel

# Exportable binary-only stage (docker build --target artifact --output ...).
FROM scratch AS artifact
COPY --from=build /clp/build/core/archive-analyzer /archive-analyzer

# Runnable image: analyzer binary + report tooling + S3 listing support.
FROM ubuntu:22.04 AS runtime

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends \
        ca-certificates \
        libcurl4 \
        python3 \
        python3-boto3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /clp/build/core/archive-analyzer /usr/local/bin/archive-analyzer
COPY --from=build /clp/components/core/src/clp_s/archive_analyzer/generate_report.py \
    /opt/archive-analyzer/generate_report.py
COPY --from=build /clp/components/core/src/clp_s/archive_analyzer/analyze.py \
    /opt/archive-analyzer/analyze.py

ENV GENERATE_REPORT_SCRIPT=/opt/archive-analyzer/generate_report.py
WORKDIR /out
ENTRYPOINT ["python3", "/opt/archive-analyzer/analyze.py"]
