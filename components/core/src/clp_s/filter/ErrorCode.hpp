#ifndef CLP_S_FILTER_ERROR_CODE_HPP
#define CLP_S_FILTER_ERROR_CODE_HPP

#include <cstdint>

#include <ystdlib/error_handling/ErrorCode.hpp>

namespace clp_s::filter {
enum class ErrorCodeEnum : uint8_t {
    InvalidFalsePositiveRate = 1,
    ParameterComputationOutOfRange,
    UnsupportedHashAlgorithm,
    CorruptFilterPayload,
    ReadFailure,
    UnsupportedFilterType,
    UnsupportedFilterNormalization,
};

using ErrorCode = ystdlib::error_handling::ErrorCode<ErrorCodeEnum>;

enum class PackedFilterErrorCodeEnum : uint8_t {
    ArchiveCountMismatch = 1,
    SerializedSizeOutOfRange,
    Truncated,
    InvalidMagicNumber,
    UnsupportedFormatVersion,
    CorruptMetadata,
};

using PackedFilterErrorCode = ystdlib::error_handling::ErrorCode<PackedFilterErrorCodeEnum>;

enum class IndexErrorCodeEnum : uint8_t {
    DuplicateIndexName = 1,
    DuplicateIndexId,
    UnknownIndexName,
    UnknownIndexId,
    UnsupportedArchiveVersion,
};

using IndexErrorCode = ystdlib::error_handling::ErrorCode<IndexErrorCodeEnum>;
}  // namespace clp_s::filter

YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(clp_s::filter::ErrorCodeEnum);
YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(clp_s::filter::PackedFilterErrorCodeEnum);
YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(clp_s::filter::IndexErrorCodeEnum);

#endif  // CLP_S_FILTER_ERROR_CODE_HPP
