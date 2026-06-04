# Telemetry

CLP collects anonymous operational metrics via [OpenTelemetry](https://opentelemetry.io/) to help
improve the software. Telemetry is **enabled by default** and can be easily disabled through
multiple mechanisms.

## Why we collect telemetry

As an open-source project, we have limited visibility into how CLP is used in the community.
Anonymous metrics help us understand deployment patterns, prioritize platform support, and
make informed build target decisions.

## What we do NOT collect

Telemetry does **not** include: log content, queries, hostnames, IP addresses, or any other
Personally Identifiable Information (PII).

## Telemetry endpoint

Metrics are exported via the [OpenTelemetry Protocol
(OTLP)](https://opentelemetry.io/docs/specs/otlp/) to:

`https://telemetry.yscope.io`

:::{note}
With the bundled OpenTelemetry Collector, `telemetry.endpoint` is the collector's export target. To
use your own collector, remove `otel_collector` from `bundled` and set `otel_collector.host` /
`otel_collector.port` to its OTLP/HTTP receiver.

When you use your own collector, the bundled collector's resource detection and batching are not
applied unless your collector config includes equivalent processors; your collector also controls
filtering, redaction, aggregation, and forwarding. For a matching baseline, start from
`components/package-template/src/etc/otel-collector/config.yaml` (Docker Compose) or the
`otel-collector-config.yaml` embedded in `tools/deployment/package-helm/templates/configmap.yaml`
(Helm).
:::

## How to disable telemetry

Any **one** of the following methods is sufficient:

### Environment variable

Set `CLP_DISABLE_TELEMETRY`, or `DO_NOT_TRACK` per the
[Console Do Not Track](https://consoledonottrack.com/) standard, before launching `start-clp.sh`.
```bash
export CLP_DISABLE_TELEMETRY=true
```

### Configuration file

::::{tab-set}
:::{tab-item} Docker Compose
:sync: docker
Edit `clp-config.yaml`:
```yaml
telemetry:
  disable: true
  endpoint: "https://telemetry.yscope.io"
```
:::
:::{tab-item} Kubernetes
:sync: k8s
Pass the config via `--set` (see the [quick-start guide](quick-start/index.md) for setup details):
```bash
helm install clp clp/clp --set clpConfig.telemetry.disable=true
```
:::
::::

### First-run prompt

When you run `start-clp.sh` for the first time in an interactive terminal, a consent prompt
appears. If you decline, `telemetry.disable: true` is written to `clp-config.yaml`.

### Network-level blocking

For network admins, block `telemetry.yscope.io` at your firewall or proxy. This is the simplest way
to disable telemetry for your entire organization.

### Interaction when multiple opt-out mechanisms are set

| Env var | Config file | First-run prompt | Network blocked | Telemetry sent?                                                        |
|---------|-------------|------------------|-----------------|------------------------------------------------------------------------|
| not set | not set     | Y (or default)   | no              | **Yes**                                                                |
| not set | not set     | N                | no              | **No** — prompt wrote `telemetry.disable: true` to config              |
| `true`  | `false`     | —                | no              | **No** — env var overrides config                                      |
| `false` | `true`      | —                | no              | **No** — `false` is not a recognized disable value; config disables it |
| not set | `false`     | —                | **yes**         | **No** — requests fail silently at the network level                   |
| `true`  | `true`      | —                | no              | **No** — both agree                                                    |
| not set | not set     | Y                | **yes**         | **No** — network blocking is independent of software settings          |

## Search query traces

`clp-s` search emits an OpenTelemetry **trace span** (`clp_s.search.archive`, instrumentation scope
`clp_s.search`, `service.name=clp-search`) for each archive searched. Because a single search fans
out to one `clp-s` process per archive, each span carries the scheduler's `clp.search.query_id` and
`clp.search.task_id` so the per-archive spans can be grouped and aggregated downstream. Each span
also includes:

- **Query shape** (no content): column-type and predicate-type counts, number of predicates, whether
  an OR clause is present, and the search time range.
- **Selectivity & counts**: total archive records, candidate records after schema matching, records
  matching the query, and the derived selectivities.
- **Termination stage**: which stage the search stopped at, plus per-stage booleans.
- **Per-schema events** (`clp.search.schema_result`): candidate/matched record counts per schema.
- **`clp.search.archive_id`** and **`clp.search.query_hash`** — a non-reversible fingerprint that
  lets identical queries be grouped without exposing their content.

Consistent with the policy above, the **raw query string is not collected**. For local debugging you
can opt in by setting `CLP_TELEMETRY_INCLUDE_QUERY` to a truthy value (`1`, `true`, `yes`, `y`) on
the query worker, which adds a `clp.search.query` attribute.

## Inspecting traces locally (and dogfooding with clp-s)

The bundled collector's traces pipeline ships with two exporters:

- `debug` — prints spans to the collector's logs (`docker logs <otel-collector>` or
  `kubectl logs -l app.kubernetes.io/component=otel-collector`).
- `file` — writes spans as OTLP-JSON, which you can compress and search with clp-s itself:
  - Docker Compose: `<logs-dir>/otel-collector-traces.jsonl` on the host.
  - Kubernetes: `/var/log/otel-collector/traces.jsonl` in the collector pod
    (`kubectl cp <pod>:/var/log/otel-collector/traces.jsonl ./traces.jsonl`).

  ```bash
  clp-s c ./telemetry-archives ./otel-collector-traces.jsonl
  clp-s s ./telemetry-archives '*: "clp_s.search.archive"'
  ```

To forward traces to your telemetry endpoint instead of inspecting them locally, switch the traces
pipeline's exporter to `otlp_http` in the collector config.
