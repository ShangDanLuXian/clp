#ifndef CLP_S_SEARCH_TELEMETRYCONTEXT_HPP
#define CLP_S_SEARCH_TELEMETRYCONTEXT_HPP

#include <memory>

namespace clp_s::search {
/**
 * RAII guard that configures the process-global OpenTelemetry tracer provider.
 *
 * On construction it installs a `TracerProvider` backed by a batch span processor and an OTLP HTTP
 * exporter so that the spans emitted by `SearchTelemetrySpan` are actually exported. On
 * destruction it force-flushes any buffered spans and shuts the provider down, which is required
 * for a short-lived CLI process to avoid dropping telemetry on exit.
 *
 * Whether telemetry is enabled is decided by the caller (via the `--enable-telemetry` command line
 * flag, which the CLP package only sets after its telemetry consent checks pass), so this guard
 * should only be constructed when telemetry is enabled. If the provider or exporter cannot be
 * constructed, a warning is logged and the API's default no-op provider is left in place, so spans
 * become no-ops.
 *
 * The exporter endpoint is resolved from (in order of precedence) the standard
 * `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`/`OTEL_EXPORTER_OTLP_ENDPOINT` environment variables, and
 * finally CLP's `CLP_TELEMETRY_ENDPOINT`. Resource attributes are picked up from
 * `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` per the OpenTelemetry specification. Setting
 * `CLP_TELEMETRY_CONSOLE_EXPORTER` to a truthy value writes spans to stderr instead of exporting
 * them, for local debugging.
 */
class TelemetryContext {
public:
    // Constructors
    TelemetryContext();

    // Delete copy constructor and assignment operator
    TelemetryContext(TelemetryContext const&) = delete;
    auto operator=(TelemetryContext const&) -> TelemetryContext& = delete;

    // Delete move constructor and assignment operator
    TelemetryContext(TelemetryContext&&) = delete;
    auto operator=(TelemetryContext&&) -> TelemetryContext& = delete;

    // Destructor
    ~TelemetryContext();

private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};
}  // namespace clp_s::search

#endif  // CLP_S_SEARCH_TELEMETRYCONTEXT_HPP
