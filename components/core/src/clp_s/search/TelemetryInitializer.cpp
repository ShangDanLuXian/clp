#include "TelemetryInitializer.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer_provider.h>
#include <spdlog/spdlog.h>

namespace otlp = opentelemetry::exporter::otlp;
namespace resource = opentelemetry::sdk::resource;
namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;

namespace clp_s::search {
namespace {
// Values (case-insensitive) that disable telemetry when assigned to `CLP_DISABLE_TELEMETRY` or
// `DO_NOT_TRACK`. Kept in sync with `_TELEMETRY_DISABLE_VALUES` in the package's `start_clp.py`.
constexpr std::array<std::string_view, 4> cTelemetryDisableValues{"1", "true", "yes", "y"};

constexpr char cServiceNameKey[]{"service.name"};
constexpr char cDefaultServiceName[]{"clp-s"};
constexpr char cTracesPath[]{"/v1/traces"};

// Bounds on how long the destructor blocks while draining buffered spans on exit. Typed as
// `seconds`; `ForceFlush`/`Shutdown` accept a `microseconds` timeout, which `seconds` converts to
// losslessly.
constexpr std::chrono::seconds cForceFlushTimeout{5};
constexpr std::chrono::seconds cShutdownTimeout{5};

/**
 * @param name
 * @return The value of the given environment variable, trimmed of surrounding whitespace and
 * lower-cased; or an empty string if the variable is unset.
 */
[[nodiscard]] auto get_trimmed_lowercase_env(char const* name) -> std::string;

/**
 * @param name
 * @return Whether the given environment variable is set to a value that requests telemetry be
 * disabled (one of `cTelemetryDisableValues`).
 */
[[nodiscard]] auto env_requests_disable(char const* name) -> bool;

/**
 * @return Whether telemetry has been disabled via `CLP_DISABLE_TELEMETRY` or `DO_NOT_TRACK`.
 */
[[nodiscard]] auto telemetry_disabled() -> bool;

/**
 * @param name
 * @return Whether the given environment variable is set to a non-empty value.
 */
[[nodiscard]] auto is_env_set(char const* name) -> bool;

/**
 * Resolves the OTLP traces endpoint from CLP-specific configuration.
 *
 * The standard `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`/`OTEL_EXPORTER_OTLP_ENDPOINT` variables are
 * honoured natively by `OtlpHttpExporterOptions`, so this only supplies an override derived from
 * CLP's `CLP_TELEMETRY_ENDPOINT` when none of the standard variables are set.
 * @return The fully-qualified traces URL, or `std::nullopt` to fall back to the exporter defaults.
 */
[[nodiscard]] auto resolve_clp_endpoint() -> std::optional<std::string>;

/**
 * @return A resource describing this process: a default `service.name` of `clp-s` (used only when
 * `OTEL_SERVICE_NAME` is unset) merged with the attributes detected from the environment.
 */
[[nodiscard]] auto make_resource() -> resource::Resource;

auto get_trimmed_lowercase_env(char const* name) -> std::string {
    char const* const raw{std::getenv(name)};
    if (nullptr == raw) {
        return {};
    }
    std::string value{raw};
    auto const not_space{[](unsigned char c) { return 0 == std::isspace(c); }};
    value.erase(value.begin(), std::ranges::find_if(value, not_space));
    value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
    std::ranges::transform(value, value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

auto env_requests_disable(char const* name) -> bool {
    auto const value{get_trimmed_lowercase_env(name)};
    return std::ranges::find(cTelemetryDisableValues, value) != cTelemetryDisableValues.end();
}

auto telemetry_disabled() -> bool {
    return env_requests_disable("CLP_DISABLE_TELEMETRY") || env_requests_disable("DO_NOT_TRACK");
}

auto is_env_set(char const* name) -> bool {
    char const* const raw{std::getenv(name)};
    return nullptr != raw && '\0' != raw[0];
}

auto resolve_clp_endpoint() -> std::optional<std::string> {
    if (is_env_set("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        || is_env_set("OTEL_EXPORTER_OTLP_ENDPOINT"))
    {
        return std::nullopt;
    }

    char const* const raw{std::getenv("CLP_TELEMETRY_ENDPOINT")};
    if (nullptr == raw || '\0' == raw[0]) {
        return std::nullopt;
    }

    std::string endpoint{raw};
    while (false == endpoint.empty() && '/' == endpoint.back()) {
        endpoint.pop_back();
    }
    if (endpoint.empty()) {
        return std::nullopt;
    }
    return endpoint + cTracesPath;
}

auto make_resource() -> resource::Resource {
    resource::ResourceAttributes attributes;
    // Only provide a default service name when the deployment hasn't set one; user-provided
    // attributes take precedence over the environment detector during `Resource::Create`.
    if (false == is_env_set("OTEL_SERVICE_NAME")) {
        attributes.SetAttribute(cServiceNameKey, cDefaultServiceName);
    }
    return resource::Resource::Create(attributes);
}
}  // namespace

class TelemetryContext::Impl {
public:
    // Constructors
    Impl() {
        if (telemetry_disabled()) {
            SPDLOG_DEBUG("Telemetry disabled; leaving the no-op tracer provider in place.");
            return;
        }

        otlp::OtlpHttpExporterOptions exporter_options;
        if (auto const endpoint{resolve_clp_endpoint()}; endpoint.has_value()) {
            exporter_options.url = *endpoint;
        }
        exporter_options.content_type = otlp::HttpRequestContentType::kBinary;

        auto exporter{otlp::OtlpHttpExporterFactory::Create(exporter_options)};
        trace_sdk::BatchSpanProcessorOptions const processor_options;
        auto processor{trace_sdk::BatchSpanProcessorFactory::Create(
                std::move(exporter),
                processor_options
        )};
        std::shared_ptr<trace_api::TracerProvider> provider{
                trace_sdk::TracerProviderFactory::Create(std::move(processor), make_resource())
        };

        m_provider = provider;
        trace_api::Provider::SetTracerProvider(provider);
    }

    // Disable copy/move constructors and assignment operators
    Impl(Impl const&) = delete;
    Impl(Impl&&) = delete;
    auto operator=(Impl const&) -> Impl& = delete;
    auto operator=(Impl&&) -> Impl& = delete;

    // Destructor
    ~Impl() {
        if (nullptr == m_provider) {
            // Telemetry was disabled (or failed to initialize); leave the default no-op provider
            // untouched.
            return;
        }
        auto* const sdk_provider{static_cast<trace_sdk::TracerProvider*>(m_provider.get())};
        sdk_provider->ForceFlush(cForceFlushTimeout);
        sdk_provider->Shutdown(cShutdownTimeout);
        std::shared_ptr<trace_api::TracerProvider> const none;
        trace_api::Provider::SetTracerProvider(none);
    }

private:
    std::shared_ptr<trace_api::TracerProvider> m_provider;
};

TelemetryContext::TelemetryContext() : m_impl{std::make_unique<Impl>()} {}

TelemetryContext::~TelemetryContext() = default;
}  // namespace clp_s::search
