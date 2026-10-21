# Context installation qualification

Qualify the development engine, signed dynamic Lance provider and Context as an
installed combination before advancing their development dependencies. This
procedure does not publish packages, create release tags or change the production
manifest.

## Source and artifact identities

The development engine in `vane-extension.toml` is
`028cd966d2f70f19d20f94cbd665d8839df1dc8b`, also pinned by the server extra in
Context's `pyproject.toml`. It includes native embedding output descriptors and
the SGLang encode adapter in addition to the unified native local runner.

Only Python, tests and documentation changed between the previous engine pin
`b51ef003415d83be042870e8580e71572f8b2c85` and this revision. An audited native
binary built from that previous pin and its matching signed provider can be
reused after checking the complete native source and build inputs. The updated
Python wheel must contain every Python module from the new revision. Preserve
the binary's original version and SourceID; do not relabel it as a new native
build or a published runtime.

Record source commits, wheel SHA-256 hashes, installed Python source comparison,
native binary hash, full engine SourceID, provider descriptor and trust identity
with the qualification results. Verify provider loading with unsigned extensions,
autoinstall and autoload disabled. A version string alone is insufficient to
establish compatibility.

## Clean installation

Build Context noneditably from a recorded commit whose engine pin matches the
development manifest. Prepare a wheelhouse and resolved requirements file for
the selected Context server, media, model SDK and test dependencies. Use the exact
engine and provider wheels together; do not resolve an unrelated indexed engine
or use a source checkout on `PYTHONPATH`.

```bash
python3.12 -m venv "$QUALIFICATION_VENV"
source "$QUALIFICATION_VENV/bin/activate"
unset PYTHONPATH
python -m pip install --no-index --find-links "$QUALIFICATION_WHEELHOUSE" \
  -r "$QUALIFICATION_REQUIREMENTS" \
  "$VANE_WHEEL" "$LANCE_PROVIDER_WHEEL" "$CONTEXT_WHEEL"
python -m pip check
```

The requirements must include the dependencies of the Context server extra and
the fixtures selected below, including `grpcio` for existing Ray tests. CI-signed
development artifacts use the `vane-ci-test-key` trust identity and remain local
test inputs.

## Separate local and Ray processes

From this Lance checkout, run the installed signed provider regression:

```bash
VANE_EXPECTED_EXTENSION_TRUST_IDENTITY=vane-ci-test-key VANE_RUNNER=local \
  python -m pytest -q python/tests/test_vane_local_batch_insert.py
```

Run the Context local acceptance from outside both source packages, substituting
an absolute path to the recorded Context checkout:

```bash
VANE_RUNNER=local python -m pytest -q --import-mode=importlib \
  "$CONTEXT_CHECKOUT/vane-context/tests/test_runner_local.py" \
  "$CONTEXT_CHECKOUT/vane-context/tests/test_video_local.py" \
  "$CONTEXT_CHECKOUT/vane-context/tests/test_embedding_outputs_local.py"
```

The local fixtures forbid Ray startup. They cover canonical `local` and its
`local-fast` alias, actual HTTP/SDK requests to authored endpoints, video
ingestion, native output storage, exact/ANN/lexical retrieval and restart. These
fixtures do not qualify live model quality, GPU execution or throughput.

Run the existing Lance Ray suites and Context's runner reopen tests in separate
pytest processes with `VANE_RUNNER` unset. Keep the existing assertions and
cluster resources. Run the engine's base release gate through its installed
test launcher, which separates non-Ray, shared-Ray and owned-cluster tests.
Record skipped optional dependencies and any initial failure with its rerun;
do not count historical results as this combination's qualification.
