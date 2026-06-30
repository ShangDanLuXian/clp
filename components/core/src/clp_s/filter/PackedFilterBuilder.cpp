#include <cstdint>
#include <span>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/filter/ErrorCode.hpp>
#include <clp_s/filter/PackedFilterBuilder.hpp>

namespace clp_s::filter {
auto PackedFilterBuilder::add_archive(
        uint16_t local_archive_id,
        clp_s::ArchiveReader const& archive_reader
) -> ystdlib::error_handling::Result<void> {
    if (local_archive_id >= m_num_archives) {
        return PackedFilterErrorCode{PackedFilterErrorCodeEnum::LocalArchiveIdOutOfRange};
    }
    for (auto const& active_index : m_active_indexes) {
        YSTDLIB_ERROR_HANDLING_TRYV(
                active_index.builder->add_archive(local_archive_id, archive_reader)
        );
    }
    return ystdlib::error_handling::success();
}

auto PackedFilterBuilder::serialize() -> ystdlib::error_handling::Result<std::vector<char>> {
    for (auto const& active_index : m_active_indexes) {
        std::vector<std::span<char const>> const archive_blobs{
                active_index.builder->get_archive_blobs()
        };
        YSTDLIB_ERROR_HANDLING_TRYV(
                m_writer.add_index(active_index.index_id, active_index.index_version, archive_blobs)
        );
    }
    return m_writer.serialize();
}
}  // namespace clp_s::filter
