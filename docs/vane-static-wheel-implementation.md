# Codex Implementation Brief: Static Lance-Bearing Vane Wheel

## Purpose

This document directs Codex to make this repository produce an opt-in Vane
wheel that statically links the Lance DuckDB extension while keeping all
Lance-specific product and build ownership out of the Vane repository. The
build uses the generic distributed scan and write contracts already present in
official Vane main and requires no Vane source patch.

The resulting artifact is a custom build of the `vane-ai` Python distribution.
It is owned, built, tested, and published by this Lance repository. It must not
be confused with the default Vane release or with the independently loadable
Lance extension built for stock DuckDB.

Read the repository-level `AGENTS.md` and the relevant development guides before
starting. Preserve unrelated work, do not use editable installs, and do not
commit, push, publish, or open a pull request unless the user explicitly asks.

## Required outcome

Implement a reproducible CI and local build path with this shape:

```text
clean, exact AstroVela/vane checkout
                    +
this lance-duckdb source tree
                    |
                    | external extension CMake configuration
                    v
      Vane native build with Lance statically linked
                    |
                    v
       custom vane-ai wheel + provenance manifest
                    |
                    v
 fresh-environment local and two-worker Ray validation
```

The installed wheel must make the following possible without downloading or
locating a separate `.duckdb_extension` file:

```sql
SELECT extension_name, install_mode
FROM duckdb_extensions()
WHERE extension_name = 'lance';

LOAD lance;
```

The first query must report Lance as `STATICALLY_LINKED`, and `LOAD lance` must
succeed with extension autoinstall and autoload disabled.

## Non-negotiable constraints

1. Do not add Lance-specific source code, build files, metadata, or tests to
   Vane. The checkout used for the build must be clean at the exact pinned
   official-main commit.
2. Do not open a Vane pull request for this work.
3. Keep all custom-wheel ownership in this repository: pins, build orchestration,
   CI, tests, documentation, artifact naming, hashes, and provenance.
4. Use a full immutable commit SHA in a clean checkout whose origin is the
   official `AstroVela/vane` repository. Verify that it is reachable from
   official `main`; do not use a moving branch name during the actual build.
5. Do not depend on a local or unpublished Vane commit.
6. Lance must be linked through DuckDB's external-extension mechanism. Do not
   vendor Lance into the Vane repository or copy Lance sources into a Vane
   checkout.
7. Preserve the existing stock-DuckDB loadable Lance artifact and its tests.
   The custom Vane wheel is an additional deliverable, not a replacement.
8. Use non-editable installs. Validate the built wheel from a fresh virtual
   environment and from outside both source trees.
9. Do not use `LANCE_DUCKDB_EXTENSION`, an explicit extension path, or
   `LANCE_DUCKDB_TEST_ALLOW_UNSIGNED` in the static-wheel acceptance tests.
10. Do not claim support that was not exercised. In particular, do not claim
    Iceberg coexistence, public manylinux compatibility, remote object-store
    success, or detached ATTACH replay unless those paths were actually tested.

If compilation requires another missing generic host API, stop and report the
exact API/ABI gap. Do not patch Vane as a workaround.

## Product and repository boundary

This work intentionally creates two independent product lanes:

| Lane | Host | Lance form | Owner | Must remain supported |
| --- | --- | --- | --- | --- |
| Stock DuckDB | Official compatible DuckDB | Loadable `.duckdb_extension` | This repository | Yes |
| Custom Vane | Exact pinned Vane source | Statically linked into `vane-ai` wheel | This repository | Yes |

The custom wheel may still be named `vane-ai` internally because that is the
upstream Python distribution name. In CI, call the uploaded artifact
`vane-lance-wheel` and attach provenance. Do not upload this wheel under the
same public package name/version as a different official Vane wheel. For the
first implementation, use a CI artifact, an internal registry with an explicit
versioning policy, or a container image. Public-package publication is out of
scope until collision and provenance policy is settled.

The pure-Python `lance-duckdb` helper package remains separate. A single Python
package that bundles both projects is not required by this task.

## Observed starting point

Revalidate every item before editing because the repository may have advanced.
At the time this brief was written:

- The repository branch was `feat/vane-main-contract-20260820` at
  `d3e7d11e8e45fd2a2bbad295c36941cb27fe35a0`.
- `vane-extension.toml` pinned Vane to
  `bdb8f819ac075733e03eaf4ff63071ccf47ea0ab`.
- `extension_config_vane.cmake` enabled `LANCE_VANE_DISTRIBUTED` and included
  the Lance extension configuration.
- `extension_config.cmake` registered Lance without `DONT_LINK`, so the
  extension can be statically linked by the host build.
- The `vane-extension-ci-tools` submodule supplied a `make vane_wheel` path that
  builds Vane with an external extension configuration and verifies the wheel.
- `.github/workflows/VaneExtension.yml` primarily tested and uploaded a
  loadable extension, so it did not yet prove the desired static-wheel product.
- `python/lance_duckdb/__init__.py` primarily expected an explicit loadable
  extension path.

Treat these as observations, not assumptions. Inspect the current code and
tooling before choosing the smallest implementation.

## Deliverables

The completed change should contain all of the following in this repository:

1. An immutable official Vane `main` commit in `vane-extension.toml`; the build
   must apply no source patch.
2. A local command, preferably the existing `make vane_wheel`, that builds the
   custom wheel using this repository's Lance sources.
3. A CI job that builds, validates, and uploads `vane-lance-wheel`.
4. A machine-readable provenance manifest beside the wheel.
5. Fresh-environment static-link smoke tests.
6. Real two-worker Ray integration coverage for the supported distributed
   Lance operations.
7. Helper behavior that can load statically linked Lance without an artifact
   path while preserving explicit-path behavior for stock DuckDB.
8. Updated repository policy and user documentation that accurately describe
   both deliverable lanes and their limits.

## Implementation sequence

### 1. Preflight and evidence capture

Before making changes:

```bash
git status --short
git branch --show-current
git rev-parse HEAD
git submodule status --recursive
git remote -v
```

Read at least these files:

- `AGENTS.md`
- `DEVELOPMENT.md`, if present
- `vane-extension.toml`
- `extension_config_vane.cmake`
- `extension_config.cmake`
- `vane-extension-ci-tools/README.md`
- `vane-extension-ci-tools/makefiles/vane_extension.Makefile`
- `vane-extension-ci-tools/scripts/vane_extension.py`
- `.github/workflows/VaneExtension.yml`
- `python/lance_duckdb/__init__.py`
- `python/tests/test_vane.py`
- `docs/vane-integration.md`

Record pre-existing changes and avoid overwriting them. Use `rg` to trace the
actual build and verification call paths rather than assuming filenames alone
define behavior.

### 2. Select and verify the Vane baseline

Fetch or query the official repository and resolve `refs/heads/main` to a full
SHA at implementation time. Verify all of the following:

- The SHA exists in `https://github.com/AstroVela/vane.git`.
- The SHA is reachable from official `main`.
- The build uses that exact SHA, not a branch or tag that can move.
- A checkout at that SHA has no tracked modifications before or after the
  custom wheel build.
- The required generic distributed extension contracts already exist there.

Update only this repository's `vane-extension.toml` pin. Include the selected
Vane SHA in CI output and in the provenance manifest.

If Lance must adapt to an upstream API change, make that compatibility change
in this repository. Keep conditional compilation narrow and explain why it is
needed. If the host lacks a necessary generic contract, stop rather than adding
a hidden Vane patch step.

### 3. Preserve the external static-link build

Prefer extending the existing `vane-extension-ci-tools` flow over creating a
second build system. The build must conceptually provide Vane with:

- the Lance source directory from this checkout;
- `extension_config_vane.cmake` or a generated equivalent;
- `LANCE_VANE_DISTRIBUTED=ON`;
- all required DuckDB dependency extensions;
- the exact Vane source identity; and
- a normal PEP 517 wheel build, never an editable install.

The generated host configuration must register Lance for static linking. Do not
set `DONT_LINK` for the Vane wheel lane. Avoid changing the stock-DuckDB loadable
configuration unless a shared compatibility fix is genuinely required.

The expected local entry point should remain simple:

```bash
git submodule update --init --recursive
make vane_wheel
```

Document any required toolchain prerequisites and the exact output directory.
The current tooling commonly writes wheels below `build/vane-wheel/dist`, but
the implementation must verify the real path rather than hard-code a stale one.

### 4. Add a static-aware Python helper path

Preserve current explicit-path behavior for users loading Lance into compatible
stock DuckDB builds. When no explicit path is supplied:

1. Query `duckdb_extensions()` for `lance`.
2. Require `install_mode = 'STATICALLY_LINKED'` (case-normalized if necessary).
3. Execute `LOAD lance` by name.
4. Fail with a clear error if Lance is neither explicitly supplied nor
   statically linked.

Do not silently run `INSTALL`, download an extension, search an untrusted
filesystem path, or enable unsigned-extension loading.

Test explicit-path and static-link modes separately. Keep the helper compatible
with the connection object types already supported by this repository.

### 5. Replace artifact-path assumptions in Vane tests

Refactor or split `python/tests/test_vane.py` so the static-wheel suite proves
runtime behavior instead of proving a serialized loadable-extension pathname.
The static suite must:

- run against the installed custom wheel, not a source-tree import;
- disable extension autoinstall and autoload;
- assert Lance is listed as `STATICALLY_LINKED`;
- run `LOAD lance` without an artifact path;
- execute representative Lance SQL;
- run supported distributed operations on a real Ray cluster with at least two
  workers; and
- verify results, not only successful plan construction.

Do not make the test depend on loadable-extension snapshot fields that are not
needed by a static extension. If the selected clean Vane `main` does not contain
those fields, that is expected for this MVP.

### 6. Build the wheel in CI

Adapt `.github/workflows/VaneExtension.yml` or add a clearly named companion
workflow. Reuse existing repository-owned build actions and scripts where
practical. The static-wheel job must:

1. Check out this repository and recursive submodules at exact commits.
2. Install documented build prerequisites.
3. Print the Lance, Vane, DuckDB fork revision, and DuckDB SourceID identities.
4. Run the repository's native/dual-build smoke coverage needed to protect the
   stock-DuckDB lane.
5. Run `make vane_wheel` (or the documented equivalent).
6. Locate exactly one expected wheel and reject zero or multiple matches.
7. Create a fresh virtual environment outside the source checkout.
8. Install exactly that wheel non-editably.
9. Install the pure-Python helper package separately if the integration tests
   need it; do not let it replace or shadow the installed `vane-ai` wheel.
10. Run the static-link smoke tests and real two-worker Ray suite.
11. Generate provenance and hashes.
12. Upload the wheel and provenance as `vane-lance-wheel`.

The validation environment must not export an extension artifact path or an
unsigned-extension override. Ensure `PYTHONPATH`, the working directory, and
installation order cannot shadow the wheel with either source tree. Print the
resolved native module path and `PRAGMA version`/SourceID evidence.

### 7. Generate provenance

Create a deterministic JSON manifest next to the wheel. Include at least:

```json
{
  "artifact_kind": "vane-ai-wheel-with-static-lance",
  "lance_repository": "<canonical repository URL>",
  "lance_commit": "<full SHA>",
  "vane_repository": "https://github.com/AstroVela/vane.git",
  "vane_commit": "<full SHA>",
  "duckdb_fork_revision": "<reported revision>",
  "duckdb_source_id": "<reported SourceID>",
  "wheel_filename": "<wheel basename>",
  "wheel_sha256": "<lowercase hex digest>",
  "target_platform": "<actual platform tag>",
  "lance_install_mode": "STATICALLY_LINKED"
}
```

Values must come from the build and installed artifact, not from duplicated
handwritten constants. Fail CI if identities disagree. Also emit a conventional
SHA-256 checksum file if the existing artifact tooling supports it.

### 8. Update documentation and policy

Update `docs/vane-integration.md` and any contradictory repository policy text,
including `AGENTS.md` if necessary. Make the distinction explicit:

- Vane supplies only its existing extension-neutral distributed contracts; no
  Lance source or product ownership moves there.
- The default Vane wheel does not contain Lance.
- This repository builds an opt-in custom Vane wheel with static Lance.
- The stock-DuckDB loadable Lance artifact continues to exist independently.
- The wheel and helper package have different responsibilities.
- The supported platform and operation matrix is limited to what CI proves.

Do not describe the custom artifact as an official Vane release.

## Functional scope for the first milestone

Prioritize a useful, honest MVP rather than expanding into every catalog and
extension combination.

Required when already supported by the generic Vane/Lance contracts:

- Open and scan a local Lance dataset through SQL.
- Exercise filter and projection behavior relevant to the integration.
- Write through `COPY ... (FORMAT LANCE)`.
- Exercise distributed `INSERT` or CTAS only if the existing generic write
  contract supports it without a Vane patch.
- Run local mode and a real Ray mode with at least two workers.

Optional only when infrastructure is available:

- MinIO/S3-compatible object storage.
- Additional supported credentials/catalog paths, with secret-redaction checks.

Explicitly out of scope for this milestone:

- Iceberg and Lance in the same wheel.
- Changes to Vane upstream.
- Loadable-extension capture or transport inside Vane plans.
- Complex `ATTACH ... (TYPE LANCE, ...)` replay after the source connection is
  destroyed, unless the selected Vane main already supports it and tests prove
  it.
- Public PyPI publication.
- Platforms not exercised by CI.

## Distributed correctness requirements

When changing Lance integration code, preserve these boundaries:

- Serialized scan, bind, and task state must contain data only, never process-
  local C++ or Rust handles.
- Workers may write data files/fragments, but the coordinator alone must commit
  catalog or dataset metadata.
- Distributed writes need a stable operation identity and idempotent retry
  behavior. An unknown commit outcome must fail closed.
- Reads must reopen the exact captured dataset version and generation. If
  external cleanup removes that snapshot, execution fails instead of switching
  to a replacement generation or newer version.
- Vector, full-text, and hybrid search must compute a globally correct top-k;
  concatenating worker-local top-k results without a global merge is invalid.
- Credentials and tokens must not be serialized in plaintext plans, printed in
  logs, or copied into artifact provenance.

If the first milestone does not touch one of these paths, state that it remains
untested rather than broadening the implementation.

## Validation matrix

Run the narrow tests first, then the repository-prescribed suites. At minimum,
capture evidence for this matrix:

| Test | Stock DuckDB loadable lane | Custom Vane static lane |
| --- | --- | --- |
| Native extension build | Required | Required |
| Extension load | Explicit signed artifact/path | `LOAD lance` by name |
| Reported mode | `LOADABLE` where applicable | `STATICALLY_LINKED` |
| Autoinstall/autoload disabled | Required | Required |
| Local Lance scan | Required | Required |
| Lance write smoke | Required where currently supported | Required for declared scope |
| Two-worker Ray execution | Not applicable | Required |
| Source connection destroyed before replay | Not required | Not claimed unless tested |
| S3/REST service integration | Claim only if service-backed | Claim only if service-backed |

Use repository commands from `AGENTS.md`. Likely commands include:

```bash
make vane_ci
make vane_wheel
```

Also run focused Python tests and the prescribed formatters. Do not substitute a
single long-lived pytest process for repository launchers that intentionally
isolate Ray clusters.

For installed-wheel validation, collect at least:

```python
import vane
from vane import _native

print(vane.__file__)
print(_native.__file__)
```

Then query the runtime engine identity and `duckdb_extensions()` from the API
actually exposed by the installed Vane version. The resolved files must be in
the fresh environment's installed package, not either source checkout.

## Acceptance criteria

The task is complete only when all applicable criteria pass:

- `git diff` contains no changes to a Vane checkout.
- This repository pins a verified, immutable official Vane `main` SHA.
- One command builds a custom wheel from a clean checkout.
- A fresh environment installs exactly that wheel non-editably.
- Lance is reported as `STATICALLY_LINKED`.
- `LOAD lance` succeeds without install, network access, artifact-path
  environment variables, or unsigned-extension settings.
- Representative local Lance SQL returns verified results.
- A real Ray cluster with at least two workers returns verified results for the
  declared distributed scope.
- The stock-DuckDB loadable artifact lane still passes its focused regressions.
- CI uploads the wheel, manifest, and checksum under an unambiguous artifact
  name.
- The manifest identities match the installed runtime.
- Documentation accurately separates proven support from untested scope.
- Formatting and all repository-required focused/base tests pass, or every
  unavailable test is reported with the exact reason.

## Stop conditions

Stop and report instead of silently widening scope if any of these occur:

- The selected Vane `main` lacks a generic API required for Lance static
  registration or the intended distributed operation.
- Building requires a tracked modification or patch in the Vane checkout.
- The custom wheel would overwrite or masquerade as an official Vane artifact
  in a public registry.
- CI cannot distinguish the installed wheel from a source-tree import.
- Only a single-process/mock-Ray test is available for a feature being claimed
  as distributed.
- A write retry or uncertain commit can produce ambiguous metadata ownership.
- Credentials would need to be embedded in serialized plans or build artifacts.

The report must include the exact failing command, error, relevant SHAs, and the
smallest missing contract. Do not “temporarily” patch Vane in CI.

## Final handoff format

When implementation is finished, report in Chinese:

1. The selected Lance and Vane full SHAs and how official Vane `main` ancestry
   was verified.
2. The files changed in this repository and the behavior each change adds.
3. The exact local and CI build commands.
4. The produced wheel filename, SHA-256, DuckDB fork revision, and SourceID.
5. Evidence that Lance is `STATICALLY_LINKED` and loads without an artifact
   path.
6. Local and real two-worker Ray test results.
7. Regression evidence for the stock-DuckDB loadable lane.
8. Any untested service/platform scope and any remaining blockers.
9. Confirmation that the official-main Vane checkout stayed clean and that no
   Vane change was made.

Do not commit, push, publish, or create a pull request as part of the handoff
unless the user separately authorizes that action.
