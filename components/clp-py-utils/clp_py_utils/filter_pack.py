from __future__ import annotations

import os
import shutil
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

PACK_MAGIC = b"CLPF"
PACK_VERSION = 1
INDEX_MAGIC = b"CLPI"
INDEX_VERSION = 1

_PACK_FOOTER_STRUCT = struct.Struct("<4sIQQQ")
_INDEX_HEADER_STRUCT = struct.Struct("<4sII")
_ENTRY_TAIL_STRUCT = struct.Struct("<QI")

PACK_FOOTER_SIZE = _PACK_FOOTER_STRUCT.size


@dataclass(frozen=True)
class FilterPackIndexEntry:
    archive_id: str
    offset: int
    size: int


@dataclass(frozen=True)
class FilterPackFooter:
    body_offset: int
    index_offset: int
    index_size: int


@dataclass(frozen=True)
class FilterPackBuildResult:
    num_filters: int
    size: int
    index_offset: int
    index_size: int


def _encode_index(entries: list[FilterPackIndexEntry]) -> bytes:
    buffer = bytearray()
    buffer += _INDEX_HEADER_STRUCT.pack(INDEX_MAGIC, INDEX_VERSION, len(entries))
    for entry in entries:
        archive_id_bytes = entry.archive_id.encode("utf-8")
        if len(archive_id_bytes) > 255:
            raise ValueError("archive_id is too long to encode")
        buffer.append(len(archive_id_bytes))
        buffer += archive_id_bytes
        buffer += _ENTRY_TAIL_STRUCT.pack(entry.offset, entry.size)
    return bytes(buffer)


def _decode_index(data: bytes) -> list[FilterPackIndexEntry]:
    if len(data) < _INDEX_HEADER_STRUCT.size:
        raise ValueError("index data is too short")
    magic, version, num_entries = _INDEX_HEADER_STRUCT.unpack_from(data, 0)
    if magic != INDEX_MAGIC:
        raise ValueError("invalid index magic")
    if version != INDEX_VERSION:
        raise ValueError(f"unsupported index version {version}")
    offset = _INDEX_HEADER_STRUCT.size
    entries: list[FilterPackIndexEntry] = []
    for _ in range(num_entries):
        if offset >= len(data):
            raise ValueError("index data truncated")
        id_len = data[offset]
        offset += 1
        end = offset + id_len
        if end > len(data):
            raise ValueError("index data truncated")
        archive_id = data[offset:end].decode("utf-8")
        offset = end
        if offset + _ENTRY_TAIL_STRUCT.size > len(data):
            raise ValueError("index data truncated")
        entry_offset, entry_size = _ENTRY_TAIL_STRUCT.unpack_from(data, offset)
        offset += _ENTRY_TAIL_STRUCT.size
        entries.append(
            FilterPackIndexEntry(
                archive_id=archive_id, offset=entry_offset, size=entry_size
            )
        )
    return entries


def _encode_footer(footer: FilterPackFooter) -> bytes:
    return _PACK_FOOTER_STRUCT.pack(
        PACK_MAGIC, PACK_VERSION, footer.body_offset, footer.index_offset, footer.index_size
    )


def _decode_footer(data: bytes) -> FilterPackFooter:
    if len(data) != PACK_FOOTER_SIZE:
        raise ValueError("footer data is wrong size")
    magic, version, body_offset, index_offset, index_size = _PACK_FOOTER_STRUCT.unpack(data)
    if magic != PACK_MAGIC:
        raise ValueError("invalid pack magic")
    if version != PACK_VERSION:
        raise ValueError(f"unsupported pack version {version}")
    return FilterPackFooter(
        body_offset=body_offset,
        index_offset=index_offset,
        index_size=index_size,
    )


def build_filter_pack(
    output_path: Path, filters: Iterable[tuple[str, Path]]
) -> FilterPackBuildResult:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    entries: list[FilterPackIndexEntry] = []
    body_offset = 0
    with open(output_path, "wb") as out_file:
        for archive_id, filter_path in filters:
            offset = out_file.tell() - body_offset
            size = filter_path.stat().st_size
            with open(filter_path, "rb") as in_file:
                shutil.copyfileobj(in_file, out_file)
            entries.append(
                FilterPackIndexEntry(archive_id=archive_id, offset=offset, size=size)
            )

        index_offset = out_file.tell()
        index_bytes = _encode_index(entries)
        out_file.write(index_bytes)
        footer = FilterPackFooter(
            body_offset=body_offset,
            index_offset=index_offset,
            index_size=len(index_bytes),
        )
        out_file.write(_encode_footer(footer))

    pack_size = output_path.stat().st_size
    return FilterPackBuildResult(
        num_filters=len(entries),
        size=pack_size,
        index_offset=index_offset,
        index_size=len(index_bytes),
    )


def read_filter_pack_footer(file_path: Path) -> FilterPackFooter:
    with open(file_path, "rb") as in_file:
        in_file.seek(-PACK_FOOTER_SIZE, os.SEEK_END)
        footer_bytes = in_file.read(PACK_FOOTER_SIZE)
    return _decode_footer(footer_bytes)


def read_filter_pack_index(file_path: Path) -> list[FilterPackIndexEntry]:
    footer = read_filter_pack_footer(file_path)
    with open(file_path, "rb") as in_file:
        in_file.seek(footer.index_offset)
        index_bytes = in_file.read(footer.index_size)
    return _decode_index(index_bytes)


def read_filter_pack_entry(
    file_path: Path, archive_id: str
) -> tuple[FilterPackIndexEntry, bytes] | None:
    footer = read_filter_pack_footer(file_path)
    entries = read_filter_pack_index(file_path)
    for entry in entries:
        if entry.archive_id == archive_id:
            with open(file_path, "rb") as in_file:
                in_file.seek(footer.body_offset + entry.offset)
                return entry, in_file.read(entry.size)
    return None
