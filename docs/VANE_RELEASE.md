# Vane provider release tooling

`vane-provider-release.toml` declares the Lance provider, the complete CPython
3.10–3.14 `manylinux_2_28_x86_64` wheel matrix, and the existing 100,000,000-byte
TestPyPI per-file upload budget. Lance has no provider dependencies; every wheel
requires the exact `vane-ai` version.

The release matrix and indexed-file checks live in the exact pinned
`vane-extension-ci-tools` submodule, not in a per-extension copy. Like the other
Vane extensions, Lance pins the tools through a committed Git submodule using
the official `AstroVela/vane-extension-ci-tools` repository. CI initializes that
submodule in every consuming job. The Make adapter and release gates require
the exact gitlink committed in the extension's `HEAD`; there is no independent
tools checkout or revision selection.

Prepare the pinned tools and run the lightweight consumer checks with Python
3.11 or newer:

```bash
git submodule update --init vane-extension-ci-tools
python -m pip install -r vane-extension-ci-tools/requirements-release.txt pytest pyyaml
python -m pytest -q python/tests/test_vane_dynamic_wheel.py python/tests/test_vane_provider_release.py
```

For a locally assembled TestPyPI candidate set:

```bash
python -I vane-extension-ci-tools/scripts/vane_provider_release.py validate \
  --manifest vane-extension.toml --extension-root . \
  --vane-source ../vane \
  --ci-tools-version "$(git rev-parse HEAD:vane-extension-ci-tools)" \
  --config vane-provider-release.toml \
  --directory build/vane-testpypi-provider-dist \
  --vane-version 0.2.0.dev612 --require-testpypi-publishable
```

The Vane checkout must already exist at the exact manifest revision. The shared
gate verifies the official Vane and tools revisions and rejects dirty or
mismatched checkouts before inspecting the wheel set. Assembly and post-upload
verification use shallow checkouts of the same Vane revision without building
native code. `verify-index --provider lance` then requires the exact indexed
wheel filenames and SHA-256 digests. Generic release edge cases are tested in
the shared repository; Lance keeps real-config, CLI/output, and workflow checks.

The existing Rust/C++ build adapter, pinned Cargo/Bison/license tooling,
artifact security verification, signing identities, and local/two-worker Ray
tests remain in this repository. The final upload still runs in the top-level
`VaneExtension.yml` with the existing `testpypi` environment and Trusted
Publisher; no publisher or signing-key reconfiguration is needed.

Shared native tools require manifest schema 2 with an explicit `[vcpkg]` table.
Both native and provider builders retain the existing integration manifest's
exact vcpkg revision; `vcpkg.json` does not select a separate baseline. This
migration does not change Vane's source revision, native dependency versions,
package versioning, or runtime behavior.
