# SPDX-License-Identifier: Apache-2.0
"""Exercise directive localization without changing static-library contents."""

import importlib.util
import struct
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "archive_exports", ROOT / "scripts/hide_msvc_archive_exports.py"
)
assert SPEC and SPEC.loader
FILTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FILTER)


def member(contents, name=b"fixture.obj/"):
    fields = (
        name.ljust(16),
        b"0".ljust(12),
        b"0".ljust(6),
        b"0".ljust(6),
        b"100644".ljust(8),
    )
    return (
        b"".join(fields)
        + str(len(contents)).encode().ljust(10)
        + b"`\n"
        + contents
        + b"\n" * (len(contents) % 2)
    )


def coff(directives, *, big=False):
    # Include a non-directive section with similar bytes to detect broad edits.
    code = b"code /EXPORT:keep_this_literal\0"
    header = bytearray(56 if big else 20)
    if big:
        struct.pack_into("<4H", header, 0, 0, 0xFFFF, 2, 0x8664)
        header[12:28] = FILTER.BIGOBJ_MAGIC
        struct.pack_into("<I", header, 44, 2)
    else:
        struct.pack_into("<2H", header, 0, 0x8664, 2)
    sections = bytearray(80)
    start = len(header) + len(sections)
    for index, (name, data, offset) in enumerate(
        [(b".text", code, start), (b".drectve", directives, start + len(code))]
    ):
        sections[index * 40 : index * 40 + 8] = name.ljust(8, b"\0")
        struct.pack_into("<II", sections, index * 40 + 16, len(data), offset)
    return bytes(header + sections) + code + directives


@pytest.mark.parametrize("big", [False, True])
@pytest.mark.parametrize("name", [b"fixture.obj/", b"/123", b"#1/9"])
def test_only_embedded_exports_change(tmp_path, big, name):
    directives = b' /DEFAULTLIB:LIBCMT /EXPORT:aws_lc_jent_rand -export:"quoted name",DATA /INCLUDE:required '
    contents = coff(directives, big=big)
    if name == b"#1/9":
        contents = b"long.obj\0" + contents
    original = b"!<arch>\n" + member(b"archive index", b"/") + member(contents, name)
    archive = tmp_path / "rust.lib"
    archive.write_bytes(original)
    expected = original.replace(
        b"/EXPORT:aws_lc_jent_rand", b" " * len(b"/EXPORT:aws_lc_jent_rand")
    ).replace(b'-export:"quoted name",DATA', b" " * len(b'-export:"quoted name",DATA'))
    assert FILTER.hide_exports(archive) == 2
    assert archive.read_bytes() == expected
    assert FILTER.hide_exports(archive) == 0
    assert archive.read_bytes() == expected


def test_utf8_directives_and_quoted_options(tmp_path):
    archive = tmp_path / "rust.lib"
    original = b"!<arch>\n" + member(
        coff(b'\xef\xbb\xbf"/EXPORT:private" /DEFAULTLIB:"C:/library path/crt.lib"\0')
    )
    archive.write_bytes(original)
    assert FILTER.hide_exports(archive) == 1
    assert archive.read_bytes() == original.replace(
        b'"/EXPORT:private"', b" " * len(b'"/EXPORT:private"')
    )


@pytest.mark.parametrize("defect", ["member", "sections", "range", "quote", "padding"])
def test_malformed_archive_is_not_modified(tmp_path, defect):
    first = member(coff(b" /EXPORT:must_remain "))
    second = bytearray(coff(b" /EXPORT:invalid "))
    if defect == "sections":
        struct.pack_into("<H", second, 2, 0xFFFF)
    elif defect == "range":
        struct.pack_into("<I", second, 20 + 40 + 20, len(second) + 100)
    elif defect == "quote":
        second = bytearray(coff(b' /EXPORT:"invalid '))
    trailing = member(second)
    if defect == "member":
        trailing = b"corrupt"
    elif defect == "padding":
        trailing = member(b"x")[:-1]
    original = b"!<arch>\n" + first + trailing
    archive = tmp_path / "rust.lib"
    archive.write_bytes(original)
    with pytest.raises(ValueError):
        FILTER.hide_exports(archive)
    assert archive.read_bytes() == original


def test_non_coff_members_and_import_objects_remain_unchanged(tmp_path):
    original = b"!<arch>\n" + member(b"BC\xc0\xde /EXPORT:bitcode_literal")
    original += member(b"\0\0\xff\xff\0\0\x64\x86" + b"\0" * 48)
    archive = tmp_path / "rust.lib"
    archive.write_bytes(original)
    assert FILTER.hide_exports(archive) == 0
    assert archive.read_bytes() == original
