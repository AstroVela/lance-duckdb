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
python -m pytest -q python/tests/test_vane_dynamic_wheel.py python/tests/test_vane_dynamic_signing.py python/tests/test_vane_provider_release.py
```

For a locally assembled TestPyPI candidate set:

```bash
python -I vane-extension-ci-tools/scripts/vane_provider_release.py validate \
  --manifest vane-extension.toml --extension-root . \
  --vane-source ../vane \
  --ci-tools-version "$(git rev-parse HEAD:vane-extension-ci-tools)" \
  --config vane-provider-release.toml \
  --directory build/vane-provider-dist \
  --vane-version 0.2.0.dev612 --channel testpypi-dev \
  --require-publishable-on testpypi
```

The Vane checkout must already exist at the exact manifest revision. The shared
gate verifies the official Vane and tools revisions and rejects dirty or
mismatched checkouts before inspecting the wheel set. Assembly and post-upload
verification use shallow checkouts of the same Vane revision without building
native code. `verify-index --index testpypi --provider lance` then requires the
exact indexed wheel filenames and SHA-256 digests. Generic release edge cases are tested in
the shared repository; Lance keeps real-config, CLI/output, and workflow checks.

The existing Rust/C++ build adapter, pinned Cargo/Bison/license tooling,
artifact security verification, signing identities, and local/two-worker Ray
tests remain in this repository. Uploads run in the top-level `VaneExtension.yml`
so that each index can identify its GitHub Trusted Publisher directly.

Shared native tools require manifest schema 2 with an explicit `[vcpkg]` table.
Both native and provider builders retain the existing integration manifest's
exact vcpkg revision; `vcpkg.json` does not select a separate baseline. This
migration does not change the development Vane source revision, native dependency versions,
package versioning, or runtime behavior.

## Development and production channels

`VaneExtension.yml` keeps `build-only` as its default operation. Pushes, pull
requests, and build-only dispatches cannot reach provider publishing or private
signing keys. They continue to build against `vane-extension.toml`, which remains
byte-for-byte pinned to the existing `vane-ai==0.2.0.dev612` runtime.

Both publishing operations require a manual dispatch on the protected
`AstroVela/lance-duckdb` `main_vane` branch. No provider tag is required or created:
the workflow uses the reviewed dispatch commit, its exact tools gitlink, and its
committed exact Vane revision.

| Operation | Runtime manifest and index | Native signature |
| --- | --- | --- |
| `testpypi-dev` | `vane-extension.toml`, TestPyPI only | `astrovela/vane-testpypi` |
| `release` | `vane-extension-release.toml`, PyPI only | `astrovela/vane` |

The production manifest currently selects preparation commit
`033b549afcb498633fd6669b26c054c00363004e`. **This is not a published Vane release.**
Production dispatch deliberately fails its secret-free preflight until a reviewed
PR changes that manifest to a canonical non-development Vane release with the
complete CPython 3.10–3.14 runtime matrix on PyPI. The selected source must include
the production-key commit. Development versions, local versions, missing or
yanked runtime wheels, and a different source/tool identity fail closed. There is
no fallback runtime, index, or signing key.

## One build, two indexes

The release channel reuses the existing candidate jobs:

1. Validate the dispatch, exact sources, canonical runtime version, production-key
   ancestry, and the complete published runtime matrix before native builds or
   signing-key access.
2. Build the native artifact once in a job with no environment, private key, or
   OIDC permission. Both testing-key CMake switches are explicitly disabled for
   production. Upload only unsigned native data and license records.
3. A separate protected signing job reads the committed exact manifest and uses
   the exact official Vane standard-library-only signing utility. It installs no
   dependencies and never builds or loads native code. It checks the bounded
   regular unsigned artifact and public DER key fingerprint, removes the key
   from the environment, and destroys its private temporary file after signing.
   Before upload it verifies a regular output of the same size, with the payload
   SHA-256 recorded before signing unchanged and only the final 256-byte signature
   slot replaced. The only output is the signed native artifact.
4. In a fresh job without secrets or OIDC, package those signed bytes without a
   native rebuild. Verify against every exact indexed runtime and assemble the full
   provider wheel matrix. Preserve provenance, checksums, licenses, and SBOM
   evidence. Independently compare signed bytes with the original unsigned bundle
   before native verification or packaging; only the signature slot may differ.
   Validate availability on both indexes without overwriting files.
5. Upload the candidate wheels to TestPyPI. Verify the complete indexed filename
   and SHA-256 set, then install from TestPyPI into fresh local and two-worker Ray
   test environments. Both tests compare the downloaded provider bytes with the
   expected build artifact; the exact production runtime comes only from PyPI.
6. After both tests pass, wait for the `pypi` environment approval. Recheck the
   complete candidate set with the shared `verify-promotion` gate in a read-only
   job without OIDC permission. The separate minimal publisher uses the same
   protected environment, so GitHub may require another approval. That job only
   downloads the original immutable artifact and uploads those **same wheels**
   using pinned actions; it does not check out code or install validation tools.
   No rebuild, re-signing, or version rewrite occurs.
7. A separate read-only job verifies the complete PyPI filename and SHA-256 set
   against those original files.

Both publishing operations use this prepare/sign/package isolation; CI-only
builds retain the `full` phase with the repository's public test-key fixture and
locally built runtime. Published builder phases reject key arguments and local
runtime packaging. There is no combined build-and-sign publishing path.

Every release stage downloads the original producer's immutable artifact ID, not
a mutable name or a verifier-selected replacement. Every artifact download fails
on a digest mismatch. `skip-existing` is used only
for retries: the shared gate must first prove that any existing index files are
byte-identical. Retry failed jobs in the same run to preserve the original
candidate artifacts; do not rebuild a partially published version. An expired
artifact or conflicting indexed file requires a new candidate, not an overwrite.

## Required production configuration

This preparation change does not configure GitHub environments or Trusted
Publishers, upload private keys, create tags, or publish packages.

- Keep the existing `testpypi` environment, TestPyPI Trusted Publisher, and
  `VANE_TESTPYPI_EXTENSION_SIGNING_PRIVATE_KEY` for development candidates.
- Configure a protected `production-signing` environment, restricted to
  `main_vane`, with required reviewer approval. Store the production private key
  there as `VANE_EXTENSION_SIGNING_PRIVATE_KEY`; never commit or attach it to a PR.
- Configure a protected `pypi` environment with required reviewer approval and
  `main_vane` as its allowed deployment branch. Prevent self-review and bypass
  where supported, so publication approval remains a separate release decision.
- Register the PyPI Trusted Publisher for project `vane-extension-lance`: owner
  `AstroVela`, repository `lance-duckdb`, workflow `VaneExtension.yml`, environment
  `pypi`. The existing TestPyPI publisher remains the staging publisher.

The production native trust identity is `astrovela/vane`. Its public
SubjectPublicKeyInfo DER SHA-256 fingerprint is
`8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb`.
This native-signature key is distinct from the TestPyPI key and from the GitHub
OIDC identity used for package-index uploads and provenance attestations.
