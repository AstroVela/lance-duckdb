# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

"""Execute the provider guide's Python examples without selecting a runner."""

import os
import re
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.real_ray,
    pytest.mark.ray_cluster_owner,
    pytest.mark.usefixtures("default_ray_runtime"),
]


def test_vane_readme_examples(tmp_path, monkeypatch):
    import vane
    from vane import runners

    assert "VANE_RUNNER" not in os.environ
    readme = Path(__file__).resolve().parents[2] / "VANE_README.md"
    markdown = readme.read_text()
    monkeypatch.chdir(tmp_path)
    namespace = {}
    executed = 0
    runner = runners.get_or_create_runner()
    counts = {"reads": 0, "writes": 0}
    original_read, original_write = runner.run_iter_tables, runner.run_write

    def read(*args, **kwargs):
        counts["reads"] += 1
        return original_read(*args, **kwargs)

    def write(*args, **kwargs):
        counts["writes"] += 1
        return original_write(*args, **kwargs)

    monkeypatch.setattr(runner, "run_iter_tables", read)
    monkeypatch.setattr(runner, "run_write", write)
    try:
        for block in re.finditer(r"^```python\n(.*?)^```", markdown, re.M | re.S):
            code = block.group(1)
            # This alternative is for a statically linked wheel. This test
            # installs a provider wheel and executes the provider path above it.
            if code.strip() == 'connection.execute("LOAD lance")':
                continue
            line = markdown.count("\n", 0, block.start(1)) + 1
            print(f"Executing VANE_README.md:{line}", flush=True)
            exec(compile("\n" * (line - 1) + code, str(readme), "exec"), namespace)
            assert runners.get_or_create_runner().name == "ray"
            assert "VANE_RUNNER" not in os.environ
            executed += 1
        assert executed >= 10
        connection = namespace["connection"]
        assert connection.sql(
            "SELECT count(*), sum(id)::BIGINT FROM lance_ns.main.source"
        ).fetchall() == [(10000, 49995000)]
        assert sorted(
            connection.sql("SELECT id, value FROM lance_ns.main.copied").fetchall()
        ) == [(i, "updated" if i < 5 else f"value-{i}") for i in range(9000)]
        assert namespace["filtered"].aggregate(
            "count(*), sum(id)::BIGINT"
        ).fetchall() == [(9900, 49990050)]
        assert connection.sql(
            "SELECT id FROM lance_vector_search('lance_demo/search.lance', "
            "'vec', [0.1, 0.2, 0.3, 0.4]::FLOAT[4], k=1, use_index=false)"
        ).fetchall() == [(10,)]
        assert connection.sql(
            "SELECT id FROM lance_fts('lance_demo/search.lance', "
            "'text', 'puppy', k=100) ORDER BY id"
        ).fetchall() == [(i,) for i in range(0, 100, 2)]
        assert connection.sql(
            "SELECT id, text FROM lance_fts('lance_demo/search.lance', "
            "'text', 'puppy', k=100) ORDER BY id"
        ).fetchall() == [(i, "playful puppy") for i in range(0, 100, 2)]
        assert connection.sql(
            "SELECT id FROM lance_hybrid_search('lance_demo/search.lance', "
            "'vec', [0.1, 0.2, 0.3, 0.4]::FLOAT[4], 'text', 'puppy', "
            "k=1, prefilter=false, alpha=0.5, oversample_factor=4)"
        ).fetchall() == [(10,)]
        assert counts["writes"] == 6, counts
        print("PASS blocks", executed, "dispatch", counts, flush=True)
    finally:
        if "connection" in namespace:
            namespace["connection"].close()
