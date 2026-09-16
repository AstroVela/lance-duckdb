# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

"""Prepare controlled Lance datasets independently of distributed writes."""

from pathlib import Path


def write_fixture_query(
    connection,
    path: str | Path,
    query: str,
    *,
    mode: str = "create",
    max_rows_per_file: int = 1024 * 1024,
    max_rows_per_group: int = 1024,
) -> None:
    import lance
    from vane import runners

    assert runners.get_or_create_runner().name == "ray"
    table = connection.sql(query).to_arrow_table()
    lance.write_dataset(
        table,
        str(path),
        mode=mode,
        max_rows_per_file=max_rows_per_file,
        max_rows_per_group=max_rows_per_group,
    )


def create_fixture_index(
    path, name, column, index_type, *, num_partitions=1, metric="l2", replace=False
):
    import lance

    dataset = lance.dataset(str(path))
    if index_type == "INVERTED":
        dataset.create_scalar_index(
            column,
            index_type,
            name=name,
            replace=replace,
            base_tokenizer="simple",
            language="English",
            stem=False,
        )
    else:
        assert index_type == "IVF_FLAT"
        dataset.create_index(
            column,
            index_type,
            name=name,
            num_partitions=num_partitions,
            metric=metric,
            replace=replace,
        )


def append_fixture_index(path, name):
    import lance

    lance.dataset(str(path)).optimize.optimize_indices(
        index_names=[name], num_indices_to_merge=0
    )


def cleanup_fixture_versions(path):
    from datetime import timedelta
    import lance

    return lance.dataset(str(path)).cleanup_old_versions(
        older_than=timedelta(seconds=0),
        delete_unverified=True,
        error_if_tagged_old_versions=False,
    )
