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

} // namespace duckdb

#endif
