#ifdef LANCE_VANE_DISTRIBUTED

#include "lance_vane_snapshot.hpp"

#include "duckdb/main/client_context_state.hpp"

#include "lance_common.hpp"
#include "lance_ffi.hpp"

namespace duckdb {

static constexpr const char *LANCE_VANE_SNAPSHOT_CACHE_STATE_KEY =
    "lance_vane_snapshot_cache_state";

class LanceVaneSnapshotCacheState final : public ClientContextState {
public:
  shared_ptr<LanceDatasetCacheEntry> Get(const string &key) {
    lock_guard<mutex> guard(lock);
    auto entry = entries.find(key);
    if (entry == entries.end()) {
      query_misses++;
      return nullptr;
    }
    query_hits++;
    return entry->second;
  }

  shared_ptr<LanceDatasetCacheEntry>
  PutOrGetExisting(const string &key,
                   shared_ptr<LanceDatasetCacheEntry> entry) {
    lock_guard<mutex> guard(lock);
    auto existing = entries.find(key);
    if (existing != entries.end()) {
      return existing->second;
    }
    entries.emplace(key, entry);
    return entry;
  }

  void QueryBegin(ClientContext &) override {
    lock_guard<mutex> guard(lock);
    query_hits = 0;
    query_misses = 0;
  }

  void QueryEnd() override {
    lock_guard<mutex> guard(lock);
    entries.clear();
  }

  void WriteProfilingInformation(std::ostream &ss) override {
    lock_guard<mutex> guard(lock);
    ss << "Lance Vane Snapshot Cache: entries=" << entries.size()
       << " hits=" << query_hits << " misses=" << query_misses << "\n";
  }

private:
  mutex lock;
  unordered_map<string, shared_ptr<LanceDatasetCacheEntry>> entries;
  idx_t query_hits = 0;
  idx_t query_misses = 0;
};

struct LanceVaneCStringDeleter {
  void operator()(const char *value) const {
    if (value) {
      lance_free_string(value);
    }
  }
};

struct LanceVaneBytesDeleter {
  size_t len;

  void operator()(uint8_t *value) const {
    if (value) {
      lance_vane_free_bytes(value, len);
    }
  }
};

static string LanceVaneSha256(const string &payload) {
  string digest(LANCE_VANE_FROZEN_SNAPSHOT_DIGEST_SIZE, '\0');
  auto *input = reinterpret_cast<const uint8_t *>(payload.data()); // NOLINT
  if (lance_vane_sha256(input, payload.size(),
                        reinterpret_cast<uint8_t *>(&digest[0])) !=
      0) { // NOLINT
    LanceConsumeLastError();
    return string();
  }
  return digest;
}

string LanceVaneDatasetSchemaFingerprint(void *dataset, const string &path,
                                         bool private_diagnostics) {
  string fingerprint(LANCE_VANE_FROZEN_SNAPSHOT_DIGEST_SIZE, '\0');
  if (lance_vane_dataset_schema_fingerprint(
          dataset,
          reinterpret_cast<uint8_t *>(&fingerprint[0])) != 0) { // NOLINT
    throw IOException("Failed to fingerprint Lance dataset schema" +
                      LanceVaneFormatErrorSuffix(path, private_diagnostics));
  }
  return fingerprint;
}

LanceVaneFrozenSnapshot LanceVaneFreezeSnapshot(void *dataset,
                                                const string &path,
                                                bool private_diagnostics) {
  uint8_t *manifest = nullptr;
  size_t manifest_len = 0;
  auto rc =
      lance_vane_serialize_dataset_manifest(dataset, &manifest, &manifest_len);
  unique_ptr<uint8_t, LanceVaneBytesDeleter> manifest_owner(
      manifest, LanceVaneBytesDeleter{manifest_len});
  if (rc != 0 || !manifest || manifest_len == 0 ||
      manifest_len > LANCE_VANE_MAX_SERIALIZED_MANIFEST_BYTES) {
    throw IOException("Failed to freeze Lance dataset manifest" +
                      LanceVaneFormatErrorSuffix(path, private_diagnostics));
  }

  LanceVaneFrozenSnapshot result;
  result.serialized_manifest.assign(
      reinterpret_cast<const char *>(manifest_owner.get()),
      manifest_len); // NOLINT
  manifest_owner.reset();
  result.manifest_sha256 = LanceVaneSha256(result.serialized_manifest);
  result.schema_fingerprint =
      LanceVaneDatasetSchemaFingerprint(dataset, path, private_diagnostics);
  if (result.manifest_sha256.empty()) {
    throw IOException("Failed to digest frozen Lance dataset manifest");
  }
  return result;
}

bool LanceVaneValidateFrozenSnapshot(const string &serialized_manifest,
                                     const string &manifest_sha256,
                                     const string &schema_fingerprint,
                                     string &out_error) {
  out_error.clear();
  if (serialized_manifest.empty()) {
    out_error = "serialized manifest is empty";
    return false;
  }
  if (serialized_manifest.size() > LANCE_VANE_MAX_SERIALIZED_MANIFEST_BYTES) {
    out_error = "serialized manifest exceeds the transport limit";
    return false;
  }
  if (manifest_sha256.size() != LANCE_VANE_FROZEN_SNAPSHOT_DIGEST_SIZE) {
    out_error = "manifest digest has an invalid size";
    return false;
  }
  if (schema_fingerprint.size() != LANCE_VANE_FROZEN_SNAPSHOT_DIGEST_SIZE) {
    out_error = "schema fingerprint has an invalid size";
    return false;
  }
  auto digest = LanceVaneSha256(serialized_manifest);
  if (digest.empty()) {
    out_error = "manifest digest could not be computed";
    return false;
  }
  if (digest != manifest_sha256) {
    out_error = "manifest digest does not match its payload";
    return false;
  }
  return true;
}

static string LanceVaneDatasetGenerationId(void *dataset, const string &path,
                                           bool private_diagnostics) {
  auto *generation_ptr = lance_dataset_generation_id(dataset);
  if (!generation_ptr) {
    throw IOException("Failed to identify Lance dataset snapshot" +
                      LanceVaneFormatErrorSuffix(path, private_diagnostics));
  }
  unique_ptr<const char, LanceVaneCStringDeleter> generation_owner(
      generation_ptr);
  string generation = generation_ptr;
  if (generation.empty()) {
    throw IOException("Lance dataset snapshot identity is empty");
  }
  return generation;
}

static string LanceVaneSnapshotCacheKey(ClientContext &context,
                                        const string &path, uint64_t version,
                                        const string &generation_id) {
  auto key = LanceBuildPathDatasetCacheKey(context, path);
  key += "|fixed-snapshot|" + to_string(version) + "|" +
         to_string(generation_id.size()) + ":" + generation_id;
  return key;
}

shared_ptr<LanceDatasetCacheEntry>
LanceVaneGetOrOpenSnapshot(ClientContext &context, const string &path,
                           uint64_t version, const string &generation_id,
                           bool private_diagnostics) {
  if (version == 0 || generation_id.empty()) {
    throw InvalidInputException(
        "Distributed Lance snapshot identity is incomplete");
  }

  auto cache_key =
      LanceVaneSnapshotCacheKey(context, path, version, generation_id);
  auto cache =
      context.registered_state->GetOrCreate<LanceVaneSnapshotCacheState>(
          LANCE_VANE_SNAPSHOT_CACHE_STATE_KEY);
  if (auto cached = cache->Get(cache_key)) {
    return cached;
  }

  auto *dataset =
      LanceOpenDatasetVersionForDistributedScan(context, path, version);
  if (!dataset) {
    throw IOException("Failed to reopen fixed Lance dataset version" +
                      LanceVaneFormatErrorSuffix(path, private_diagnostics));
  }
  auto entry = make_shared_ptr<LanceDatasetCacheEntry>(dataset, path);
  if (lance_dataset_version(dataset) != version) {
    throw IOException("Reopened Lance dataset version does not match the "
                      "coordinator snapshot");
  }
  if (LanceVaneDatasetGenerationId(dataset, path, private_diagnostics) !=
      generation_id) {
    throw IOException("Distributed Lance snapshot generation changed; "
                      "generation does not match the coordinator snapshot");
  }
  return cache->PutOrGetExisting(cache_key, entry);
}

shared_ptr<LanceDatasetCacheEntry> LanceVaneGetOrOpenFrozenSnapshot(
    ClientContext &context, const string &path, uint64_t version,
    const string &generation_id, const string &serialized_manifest,
    const string &manifest_sha256, const string &schema_fingerprint,
    bool private_diagnostics) {
  if (version == 0 || generation_id.empty()) {
    throw InvalidInputException(
        "Distributed Lance snapshot identity is incomplete");
  }
  // Worker-bind construction or deserialization already validates the
  // envelope digest and bounded fields. Do not hash a large manifest again at
  // scan initialization.

  auto cache_key =
      LanceVaneSnapshotCacheKey(context, path, version, generation_id);
  cache_key += "|manifest-sha256|" + to_string(manifest_sha256.size()) + ":" +
               manifest_sha256;
  cache_key += "|schema-fingerprint|" + to_string(schema_fingerprint.size()) +
               ":" + schema_fingerprint;
  auto cache =
      context.registered_state->GetOrCreate<LanceVaneSnapshotCacheState>(
          LANCE_VANE_SNAPSHOT_CACHE_STATE_KEY);
  if (auto cached = cache->Get(cache_key)) {
    return cached;
  }

  auto *dataset = LanceOpenDatasetVersionFromManifestForDistributedScan(
      context, path, version, serialized_manifest, generation_id);
  if (!dataset) {
    throw IOException("Failed to open coordinator-frozen Lance snapshot" +
                      LanceVaneFormatErrorSuffix(path, private_diagnostics));
  }
  auto entry = make_shared_ptr<LanceDatasetCacheEntry>(dataset, path);
  if (lance_dataset_version(dataset) != version) {
    throw IOException("Frozen Lance dataset version does not match the "
                      "coordinator snapshot");
  }
  if (LanceVaneDatasetGenerationId(dataset, path, private_diagnostics) !=
      generation_id) {
    throw IOException("Distributed Lance snapshot generation changed; "
                      "generation does not match the coordinator snapshot");
  }
  if (LanceVaneDatasetSchemaFingerprint(dataset, path, private_diagnostics) !=
      schema_fingerprint) {
    throw IOException("Frozen Lance dataset schema does not match the "
                      "coordinator snapshot");
  }
  return cache->PutOrGetExisting(cache_key, entry);
}

shared_ptr<LanceDatasetCacheEntry> LanceVaneGetOrOpenFrozenSearchSnapshot(
    ClientContext &context, const string &path, uint64_t version,
    const string &generation_id, const string &serialized_manifest,
    const string &manifest_sha256, const string &serialized_index_section,
    const string &index_section_sha256, const string &schema_fingerprint,
    bool private_diagnostics) {
  if (version == 0 || generation_id.empty() ||
      index_section_sha256.size() != LANCE_VANE_FROZEN_SNAPSHOT_DIGEST_SIZE) {
    throw InvalidInputException(
        "Distributed Lance search snapshot identity is incomplete");
  }

  auto cache_key =
      LanceVaneSnapshotCacheKey(context, path, version, generation_id);
  cache_key += "|manifest-sha256|" + to_string(manifest_sha256.size()) + ":" +
               manifest_sha256;
  cache_key += "|schema-fingerprint|" + to_string(schema_fingerprint.size()) +
               ":" + schema_fingerprint;
  cache_key += "|index-section-sha256|" +
               to_string(index_section_sha256.size()) + ":" +
               index_section_sha256;
  auto cache =
      context.registered_state->GetOrCreate<LanceVaneSnapshotCacheState>(
          LANCE_VANE_SNAPSHOT_CACHE_STATE_KEY);
  if (auto cached = cache->Get(cache_key)) {
    return cached;
  }

  auto *dataset =
      LanceOpenDatasetVersionFromManifestAndIndexSectionForDistributedSearch(
          context, path, version, serialized_manifest, serialized_index_section,
          generation_id);
  if (!dataset) {
    throw IOException(
        "Failed to open coordinator-frozen Lance search snapshot" +
        LanceVaneFormatErrorSuffix(path, private_diagnostics));
  }
  auto entry = make_shared_ptr<LanceDatasetCacheEntry>(dataset, path);
  if (lance_dataset_version(dataset) != version) {
    throw IOException("Frozen Lance search dataset version does not match the "
                      "coordinator snapshot");
  }
  if (LanceVaneDatasetGenerationId(dataset, path, private_diagnostics) !=
      generation_id) {
    throw IOException("Distributed Lance search snapshot generation changed; "
                      "generation does not match the coordinator snapshot");
  }
  if (LanceVaneDatasetSchemaFingerprint(dataset, path, private_diagnostics) !=
      schema_fingerprint) {
    throw IOException("Frozen Lance search dataset schema does not match the "
                      "coordinator snapshot");
  }
  return cache->PutOrGetExisting(cache_key, entry);
}

} // namespace duckdb

#endif
