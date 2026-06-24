#include "ErrorCode.hpp"

#include <string>

#include <ystdlib/error_handling/ErrorCode.hpp>

namespace {
using clp_s::filter::ErrorCodeEnum;
using clp_s::filter::IndexErrorCodeEnum;
using clp_s::filter::PackedFilterErrorCodeEnum;
using ErrorCategory = ystdlib::error_handling::ErrorCategory<ErrorCodeEnum>;
using IndexErrorCategory = ystdlib::error_handling::ErrorCategory<IndexErrorCodeEnum>;
using PackedFilterErrorCategory = ystdlib::error_handling::ErrorCategory<PackedFilterErrorCodeEnum>;
}  // namespace

template <>
auto ErrorCategory::name() const noexcept -> char const* {
    return "clp_s::filter::ErrorCode";
}

template <>
auto ErrorCategory::message(ErrorCodeEnum error_enum) const -> std::string {
    switch (error_enum) {
        case ErrorCodeEnum::InvalidFalsePositiveRate:
            return "false-positive rate must be in the range [1e-6, 1)";
        case ErrorCodeEnum::ParameterComputationOutOfRange:
            return "bloom filter parameter computation overflowed or produced invalid values";
        case ErrorCodeEnum::UnsupportedHashAlgorithm:
            return "bloom filter hash algorithm is unsupported";
        case ErrorCodeEnum::CorruptFilterPayload:
            return "bloom filter payload is malformed or inconsistent";
        case ErrorCodeEnum::ReadFailure:
            return "failed to read filter data from reader";
        case ErrorCodeEnum::UnsupportedFilterType:
            return "filter type is unsupported";
        case ErrorCodeEnum::UnsupportedFilterNormalization:
            return "filter normalization is unsupported";
    }
    return "unknown error code enum";
}

template <>
auto PackedFilterErrorCategory::name() const noexcept -> char const* {
    return "clp_s::filter::PackedFilterErrorCode";
}

template <>
auto PackedFilterErrorCategory::message(PackedFilterErrorCodeEnum error_enum) const -> std::string {
    switch (error_enum) {
        case PackedFilterErrorCodeEnum::ArchiveCountMismatch:
            return "the number of archive blobs does not match the number of archives";
        case PackedFilterErrorCodeEnum::SerializedSizeOutOfRange:
            return "a serialized size exceeds the format's field width";
        case PackedFilterErrorCodeEnum::Truncated:
            return "the serialized pack is truncated";
        case PackedFilterErrorCodeEnum::InvalidMagicNumber:
            return "the magic number does not match a Packed Filter";
        case PackedFilterErrorCodeEnum::UnsupportedFormatVersion:
            return "the Packed Filter format major version is unsupported";
        case PackedFilterErrorCodeEnum::CorruptMetadata:
            return "the Packed Filter metadata is malformed";
        case PackedFilterErrorCodeEnum::LocalArchiveIdOutOfRange:
            return "the local archive id is outside the range of indexed archives";
    }
    return "unknown error code enum";
}

template <>
auto IndexErrorCategory::name() const noexcept -> char const* {
    return "clp_s::filter::IndexErrorCode";
}

template <>
auto IndexErrorCategory::message(IndexErrorCodeEnum error_enum) const -> std::string {
    switch (error_enum) {
        case IndexErrorCodeEnum::DuplicateIndexName:
            return "an index is already registered with the given name";
        case IndexErrorCodeEnum::DuplicateIndexId:
            return "an index is already registered with the given Index ID";
        case IndexErrorCodeEnum::UnknownIndexName:
            return "no index is registered with the given name";
        case IndexErrorCodeEnum::UnknownIndexId:
            return "no index is registered with the given Index ID";
        case IndexErrorCodeEnum::UnsupportedArchiveVersion:
            return "no registered index builder supports the archive version";
    }
    return "unknown error code enum";
}
