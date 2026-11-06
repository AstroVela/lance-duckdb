#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Keep Rust static-library definitions private when linking a Vane PE DLL.

MSVC combines .def exports with /EXPORT directives embedded by dependencies'
__declspec(dllexport) declarations. Blank those directives in COFF .drectve
sections before linking. Archive/member offsets, code, symbols and other linker
directives (including CRT selection) retain their original bytes.
"""

from __future__ import annotations

import argparse
import struct
from pathlib import Path
from typing import BinaryIO

BIGOBJ_MAGIC = bytes.fromhex("c7a1bad1eebaa94baf20faf66aa4dcb8")
MAX_SECTIONS = 1_000_000
MAX_DIRECTIVES = 1024 * 1024


def _export_spans(contents: bytes):
    position = 3 if contents.startswith(b"\xef\xbb\xbf") else 0
    separators = b" \t\r\n\x00"
    while position < len(contents):
        if contents[position] in separators:
            position += 1
            continue
        start = position
        quoted = False
        while position < len(contents):
            value = contents[position]
            if value == ord('"'):
                quoted = not quoted
            elif not quoted and value in separators:
                break
            position += 1
        if quoted:
            raise ValueError("unterminated quote in COFF linker directives")
        token = contents[start:position].strip(b'"').lower()
        if token.startswith((b"/export:", b"-export:")):
            yield start, position


def _member_exports(stream: BinaryIO, offset: int, size: int):
    stream.seek(offset)
    header = stream.read(min(size, 56))
    if len(header) >= 20 and struct.unpack_from("<H", header)[0] in {0x8664, 0xAA64}:
        section_count = struct.unpack_from("<H", header, 2)[0]
        if struct.unpack_from("<H", header, 16)[0] != 0:
            raise ValueError("expected a COFF object without an optional header")
        section_offset = 20
    elif (
        len(header) >= 56
        and header[:4] == b"\0\0\xff\xff"
        and header[12:28] == BIGOBJ_MAGIC
    ):
        section_count = struct.unpack_from("<I", header, 44)[0]
        section_offset = 56
    else:
        # Archive indices, import objects and LLVM bitcode have no COFF sections.
        return
    if section_count > MAX_SECTIONS or section_offset + section_count * 40 > size:
        raise ValueError("invalid COFF section table")
    table_end = section_offset + section_count * 40
    stream.seek(offset + section_offset)
    sections = stream.read(section_count * 40)
    for index in range(section_count):
        section = sections[index * 40 : (index + 1) * 40]
        if section[:8] != b".drectve":
            continue
        data_size, data_offset = struct.unpack_from("<II", section, 16)
        if data_size == 0:
            continue
        if (
            data_size > MAX_DIRECTIVES
            or data_offset < table_end
            or data_offset + data_size > size
        ):
            raise ValueError("out-of-bounds COFF linker-directive section")
        stream.seek(offset + data_offset)
        contents = stream.read(data_size)
        for start, end in _export_spans(contents):
            yield offset + data_offset + start, end - start


def hide_exports(path: Path) -> int:
    with path.open("r+b") as stream:
        size = stream.seek(0, 2)
        stream.seek(0)
        if stream.read(8) != b"!<arch>\n":
            raise ValueError("expected an MSVC static archive")
        position = 8
        patches = []
        while position < size:
            stream.seek(position)
            header = stream.read(60)
            if len(header) != 60 or header[58:] != b"`\n":
                raise ValueError("invalid static-archive member header")
            member_size = int(header[48:58].strip())
            member_start = position + 60
            if member_size < 0 or member_start + member_size > size:
                raise ValueError("out-of-bounds static-archive member")
            name = header[:16].strip()
            if name.startswith(b"#1/"):
                name_size = int(name[3:])
                if not 0 <= name_size <= member_size:
                    raise ValueError("invalid static-archive extended name")
                object_start = member_start + name_size
                object_size = member_size - name_size
            else:
                object_start, object_size = member_start, member_size
            if name not in {b"/", b"//", b"/SYM64/"}:
                patches.extend(_member_exports(stream, object_start, object_size))
            position = member_start + member_size + member_size % 2
        if position != size:
            raise ValueError("truncated static-archive padding")
        # Validate the complete archive before applying any edit.
        for offset, length in patches:
            stream.seek(offset)
            stream.write(b" " * length)
    return len(patches)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()
    print(
        f"Localized {hide_exports(args.archive)} embedded export directives in {args.archive.name}"
    )
