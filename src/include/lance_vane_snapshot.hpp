#pragma once

#ifdef LANCE_VANE_DISTRIBUTED

#include "duckdb.hpp"

#include "lance_dataset_cache.hpp"

namespace duckdb {

static constexpr idx_t LANCE_VANE_FROZEN_SNAPSHOT_VERSION = 1;
static constexpr idx_t LANCE_VANE_FROZEN_SNAPSHOT_DIGEST_SIZE = 32;
// Keep this limit in sync with MAX_SERIALIZED_MANIFEST_BYTES in
// rust/ffi/dataset.rs. It bounds detached-plan allocation and transport.
static constexpr idx_t LANCE_VANE_MAX_SERIALIZED_MANIFEST_BYTES =
    256ULL * 1024ULL * 1024ULL;

struct LanceVaneFrozenSnapshot {
  string serialized_manifest;
  string manifest_sha256;
  string schema_fingerprint;
};

LanceVaneFrozenSnapshot LanceVaneFreezeSnapshot(void *dataset,
                                                const string &path,
                                                bool private_diagnostics);

bool LanceVaneValidateFrozenSnapshot(const string &serialized_manifest,
                                     const string &manifest_sha256,
                                     const string &schema_fingerprint,
                                     string &out_error);

string LanceVaneDatasetSchemaFingerprint(void *dataset, const string &path,
                                         bool private_diagnostics);

shared_ptr<LanceDatasetCacheEntry>
LanceVaneGetOrOpenSnapshot(ClientContext &context, const string &path,
                           uint64_t version, const string &generation_id,
                           bool private_diagnostics);

shared_ptr<LanceDatasetCacheEntry> LanceVaneGetOrOpenFrozenSnapshot(
    ClientContext &context, const string &path, uint64_t version,
    const string &generation_id, const string &serialized_manifest,
    const string &manifest_sha256, const string &schema_fingerprint,
    bool private_diagnostics);

} // namespace duckdb

#endif
