# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Helpers for exercising the installed Lance provider wheel."""

from __future__ import annotations

import os
from importlib import import_module
from importlib.metadata import entry_points


def load_packaged_dynamic_lance(connection: object) -> None:
    """Validate and load the exact installed Lance provider."""
    import vane
    from vane.extensions import LocalExtensionProvider

    trust_identity = os.environ.get("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY")
    if not trust_identity:
        raise AssertionError(
            "VANE_EXPECTED_EXTENSION_TRUST_IDENTITY must name the test trust root"
        )

    matches = [
        candidate
        for candidate in entry_points(group="vane.dynamic_extension_providers")
        if candidate.name == "lance"
    ]
    if len(matches) != 1:
        raise AssertionError(
            f"expected one installed 'lance' provider, found {len(matches)}"
        )
    entry_point = matches[0]
    provider = entry_point.load()()
    if not isinstance(provider, LocalExtensionProvider):
        raise TypeError("the 'lance' entry point did not return LocalExtensionProvider")
    descriptor = import_module(entry_point.module).descriptor()
    if descriptor.name != "lance":
        raise AssertionError(
            f"the 'lance' provider returned descriptor for {descriptor.name!r}"
        )
    if descriptor.trust_identity != trust_identity:
        raise AssertionError(
            f"the Lance descriptor uses unexpected trust identity "
            f"{descriptor.trust_identity!r}"
        )
    if descriptor.dependencies:
        raise AssertionError("the self-contained Lance provider has dependencies")
    artifact = provider.find(descriptor.identity)
    if artifact is None or artifact.descriptor != descriptor:
        raise AssertionError("the Lance provider does not own its descriptor")

    security = connection.execute(
        """
        SELECT
            CAST(current_setting('allow_unsigned_extensions') AS BOOLEAN),
            CAST(current_setting('autoinstall_known_extensions') AS BOOLEAN),
            CAST(current_setting('autoload_known_extensions') AS BOOLEAN)
        """
    ).fetchone()
    if security != (False, False, False):
        raise AssertionError(
            f"dynamic extension security settings are not fail-closed: {security!r}"
        )
    state = connection.execute(
        "SELECT loaded, installed, install_mode FROM duckdb_extensions() "
        "WHERE extension_name = 'lance'"
    ).fetchone()
    if state not in (None, (False, False, "NOT_INSTALLED")):
        raise AssertionError(
            f"Lance was installed or linked before provider loading: {state!r}"
        )

    resolved = vane.load_installed_extension("lance", connection=connection)
    if resolved.descriptor != descriptor:
        raise AssertionError("provider loading returned a different Lance descriptor")
    loaded = connection.execute(
        "SELECT loaded, installed, install_mode FROM duckdb_extensions() "
        "WHERE extension_name = 'lance'"
    ).fetchone()
    if loaded != (True, False, "NOT_INSTALLED"):
        raise AssertionError(f"Lance did not load dynamically: {loaded!r}")


__all__ = ["load_packaged_dynamic_lance"]
