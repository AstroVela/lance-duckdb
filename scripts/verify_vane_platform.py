#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Qualify an installed signed Lance provider with local and Ray execution."""

from __future__ import annotations

import argparse
import os
from importlib.metadata import entry_points
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", choices=("local", "ray"), required=True)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    os.environ["VANE_RUNNER"] = args.runner
    import vane

    providers = [
        entry
        for entry in entry_points(group="vane.dynamic_extension_providers")
        if entry.name == "lance"
    ]
    if len(providers) != 1:
        raise RuntimeError("expected exactly one installed Lance provider")
    provider = providers[0].load()()
    if provider.trust_identity != os.environ["VANE_EXPECTED_EXTENSION_TRUST_IDENTITY"]:
        raise RuntimeError("unexpected Lance signing identity")
    args.directory.mkdir(parents=True)
    connection = vane.connect(
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        }
    )
    try:
        security = connection.execute(
            "SELECT current_setting('allow_unsigned_extensions'), current_setting('allow_extensions_metadata_mismatch')"
        ).fetchone()
        if security != (False, False):
            raise RuntimeError(
                "extension signature and identity checks must remain enabled"
            )
        vane.load_installed_extension("lance", connection=connection)
        namespace = args.directory / "mutation path 更新"
        namespace.mkdir()
        root = namespace.resolve().as_posix().replace("'", "''")
        connection.execute(f"ATTACH '{root}' AS lance_platform (TYPE LANCE)")
        connection.sql(
            "SELECT i::BIGINT AS id, 'original'::VARCHAR AS value "
            "FROM range(8) AS source(i)"
        ).create("lance_platform.main.records")
        connection.sql(
            "SELECT 8::BIGINT AS id, 'original'::VARCHAR AS value"
        ).insert_into("lance_platform.main.records")
        rows = connection.execute(
            "SELECT count(*), sum(id)::BIGINT FROM lance_platform.main.records"
        ).fetchall()
        if rows != [(9, 36)]:
            raise RuntimeError(
                f"incorrect {args.runner} Lance read/write result: {rows}"
            )
        connection.table("lance_platform.main.records").update(
            {"value": vane.lit("updated")}, condition=vane.col("id") < 3
        )
        rows = connection.execute(
            "SELECT id, value FROM lance_platform.main.records ORDER BY id"
        ).fetchall()
        if rows != [(i, "updated" if i < 3 else "original") for i in range(9)]:
            raise RuntimeError(f"incorrect {args.runner} Lance UPDATE result: {rows}")

        # The append is a whole fragment; id=1 also exercises a deletion file.
        connection.table("lance_platform.main.records").delete(
            condition=vane.col("id") == 8
        )
        connection.table("lance_platform.main.records").delete(
            condition=vane.col("id") == 1
        )
        rows = connection.execute(
            "SELECT id, value FROM lance_platform.main.records ORDER BY id"
        ).fetchall()
        if rows != [
            (i, "updated" if i < 3 else "original") for i in range(8) if i != 1
        ]:
            raise RuntimeError(f"incorrect {args.runner} Lance DELETE result: {rows}")

        connection.table("lance_platform.main.records").update(
            {"value": vane.lit("all-updated")}
        )
        rows = connection.execute(
            "SELECT id, value FROM lance_platform.main.records ORDER BY id"
        ).fetchall()
        if rows != [(i, "all-updated") for i in range(8) if i != 1]:
            raise RuntimeError(
                f"incorrect {args.runner} Lance full UPDATE result: {rows}"
            )
        connection.table("lance_platform.main.records").delete()
        if connection.execute(
            "SELECT count(*) FROM lance_platform.main.records"
        ).fetchone() != (0,):
            raise RuntimeError(f"incorrect {args.runner} Lance full DELETE result")
    finally:
        connection.close()
        vane.teardown_runner()


if __name__ == "__main__":
    main()
