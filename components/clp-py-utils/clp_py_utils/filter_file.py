from __future__ import annotations

import hashlib
import struct
import sys
from dataclasses import dataclass


FILTER_FILE_MAGIC = b"CLPF"
FILTER_FILE_VERSION = 1

FILTER_TYPE_NONE = 0
FILTER_TYPE_BLOOM_V1 = 1

FILTER_FLAG_NORMALIZED = 0x1

_FILTER_HEADER_STRUCT = struct.Struct("<4sIBBHdQ")
_BLOOM_HEADER_STRUCT = struct.Struct("<IQQ")
_SIZE_T_BYTES = struct.calcsize("P")
_SIZE_T_MASK = (1 << (_SIZE_T_BYTES * 8)) - 1


@dataclass(frozen=True)
class BloomFilterData:
    num_hash_functions: int
    bit_array_size: int
    bit_array: bytes


@dataclass(frozen=True)
class FilterFileData:
    filter_type: int
    normalize: bool
    false_positive_rate: float
    num_elements: int
    bloom: BloomFilterData | None


def read_filter_file_bytes(data: bytes) -> FilterFileData:
    if len(data) < _FILTER_HEADER_STRUCT.size:
        raise ValueError("filter file data is too short")

    (
        magic,
        version,
        filter_type,
        flags,
        _reserved,
        fpr,
        num_elements,
    ) = _FILTER_HEADER_STRUCT.unpack_from(data, 0)

    if magic != FILTER_FILE_MAGIC:
        raise ValueError("invalid filter file magic")
    if version != FILTER_FILE_VERSION:
        raise ValueError(f"unsupported filter file version {version}")

    normalize = (flags & FILTER_FLAG_NORMALIZED) != 0

    if filter_type == FILTER_TYPE_NONE:
        return FilterFileData(
            filter_type=filter_type,
            normalize=normalize,
            false_positive_rate=fpr,
            num_elements=num_elements,
            bloom=None,
        )

    offset = _FILTER_HEADER_STRUCT.size
    if filter_type != FILTER_TYPE_BLOOM_V1:
        return FilterFileData(
            filter_type=filter_type,
            normalize=normalize,
            false_positive_rate=fpr,
            num_elements=num_elements,
            bloom=None,
        )

    if len(data) < offset + _BLOOM_HEADER_STRUCT.size:
        raise ValueError("filter file bloom header is truncated")

    num_hash_functions, bit_array_size, bit_array_bytes = _BLOOM_HEADER_STRUCT.unpack_from(
        data, offset
    )
    offset += _BLOOM_HEADER_STRUCT.size

    end = offset + bit_array_bytes
    if end > len(data):
        raise ValueError("filter file bloom data is truncated")

    bloom = BloomFilterData(
        num_hash_functions=num_hash_functions,
        bit_array_size=bit_array_size,
        bit_array=data[offset:end],
    )
    return FilterFileData(
        filter_type=filter_type,
        normalize=normalize,
        false_positive_rate=fpr,
        num_elements=num_elements,
        bloom=bloom,
    )


def filter_might_contain(
    filter_data: FilterFileData,
    tokens: list[str],
    ignore_case: bool,
) -> bool:
    if filter_data.filter_type == FILTER_TYPE_NONE:
        return True
    if filter_data.bloom is None:
        return True
    if ignore_case and not filter_data.normalize:
        return True

    normalized_tokens = tokens
    if filter_data.normalize:
        normalized_tokens = [token.lower() for token in tokens]

    bloom = filter_data.bloom
    for token in normalized_tokens:
        if not bloom_might_contain(bloom, token):
            return False
    return True


def bloom_might_contain(bloom: BloomFilterData, token: str) -> bool:
    if bloom.bit_array_size == 0 or not bloom.bit_array:
        return False
    if bloom.num_hash_functions == 0:
        return False

    h1, h2 = _compute_hashes(token)
    for i in range(bloom.num_hash_functions):
        hash_value = (h1 + i * h2) & _SIZE_T_MASK
        bit_index = hash_value % bloom.bit_array_size
        byte_index = bit_index // 8
        bit_offset = bit_index % 8
        if (bloom.bit_array[byte_index] & (1 << bit_offset)) == 0:
            return False
    return True


def _compute_hashes(token: str) -> tuple[int, int]:
    value_bytes = token.encode("utf-8")
    h1 = hashlib.sha256(value_bytes).digest()
    h2 = hashlib.sha256(value_bytes + b"_bloom_").digest()
    return _hash_to_size_t(h1), _hash_to_size_t(h2)


def _hash_to_size_t(digest: bytes) -> int:
    bytes_to_use = digest[:_SIZE_T_BYTES]
    return int.from_bytes(bytes_to_use, byteorder=sys.byteorder, signed=False)
