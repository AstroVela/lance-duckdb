#include "lance_session_state.hpp"

#include "lance_common.hpp"
#include "lance_ffi.hpp"

#include <atomic>

namespace duckdb {

static constexpr const char *LANCE_SESSION_STATE_KEY = "lance_session_state";
static constexpr const char *LANCE_SHARED_SESSION_CACHE_KEY =
    "lance.shared_session.v1";
static std::atomic<uint64_t> LANCE_SHARED_SESSION_ID_GEN{1};

#ifdef LANCE_VANE_DISTRIBUTED
static constexpr const char *LANCE_VANE_INDEX_CACHE_SIZE_SETTING =
    "lance_vane_index_cache_size_bytes";
static constexpr const char *LANCE_VANE_METADATA_CACHE_SIZE_SETTING =
    "lance_vane_metadata_cache_size_bytes";

LanceSharedSessionEntry::LanceSharedSessionEntry(
    uint64_t index_cache_size_bytes_p, uint64_t metadata_cache_size_bytes_p)
    : index_cache_size_bytes(index_cache_size_bytes_p),
      metadata_cache_size_bytes(metadata_cache_size_bytes_p) {
#else
LanceSharedSessionEntry::LanceSharedSessionEntry() {
#endif
  session_id = LANCE_SHARED_SESSION_ID_GEN.fetch_add(1);
  session = lance_create_session(
#ifdef LANCE_VANE_DISTRIBUTED
      index_cache_size_bytes, metadata_cache_size_bytes
#else
      0, 0
#endif
  );
  if (!session) {
    throw IOException("Failed to create Lance session" +
                      LanceFormatErrorSuffix());
  }
}

LanceSharedSessionEntry::~LanceSharedSessionEntry() {
  if (session) {
    lance_close_session(session);
    session = nullptr;
  }
}

string LanceSharedSessionEntry::ObjectType() { return "lance_shared_session"; }

string LanceSharedSessionEntry::GetObjectType() { return ObjectType(); }

optional_idx LanceSharedSessionEntry::GetEstimatedCacheMemory() const {
  return optional_idx{};
}

#ifdef LANCE_VANE_DISTRIBUTED
static uint64_t LanceVaneSessionCacheSize(ClientContext &context,
                                          const string &name,
                                          uint64_t default_value) {
  Value value;
  if (!context.TryGetCurrentSetting(name, value) || value.IsNull()) {
    return default_value;
  }
  auto result = value.DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
  if (result == 0) {
    throw InvalidInputException("%s must be greater than zero", name);
  }
  return result;
}

static void ValidateLanceVaneSessionCacheSetting(ClientContext &context,
                                                 SetScope scope,
                                                 Value &parameter,
                                                 const string &name,
                                                 bool index_cache) {
  if (scope != SetScope::GLOBAL && scope != SetScope::AUTOMATIC) {
    throw InvalidInputException("%s is a database-global setting", name);
  }
  if (parameter.IsNull()) {
    throw InvalidInputException("%s cannot be NULL", name);
  }
  auto requested =
      parameter.DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
  if (requested == 0) {
    throw InvalidInputException("%s must be greater than zero", name);
  }

  auto &cache = ObjectCache::GetObjectCache(context);
  auto existing =
      cache.Get<LanceSharedSessionEntry>(LANCE_SHARED_SESSION_CACHE_KEY);
  if (!existing) {
    return;
  }
  auto configured = index_cache ? existing->IndexCacheSizeBytes()
                                : existing->MetadataCacheSizeBytes();
  if (configured != requested) {
    throw InvalidInputException(
        "%s must be configured before the first Lance access", name);
  }
}

static void SetLanceVaneIndexCacheSize(ClientContext &context, SetScope scope,
                                       Value &parameter) {
  ValidateLanceVaneSessionCacheSetting(
      context, scope, parameter, LANCE_VANE_INDEX_CACHE_SIZE_SETTING, true);
}

static void SetLanceVaneMetadataCacheSize(ClientContext &context,
                                          SetScope scope, Value &parameter) {
  ValidateLanceVaneSessionCacheSetting(
      context, scope, parameter, LANCE_VANE_METADATA_CACHE_SIZE_SETTING, false);
}

void RegisterLanceVaneSessionOptions(DBConfig &config) {
  config.AddExtensionOption(
      LANCE_VANE_INDEX_CACHE_SIZE_SETTING,
      "Maximum bytes retained by the process-local Lance index cache",
      LogicalType::UBIGINT,
      Value::UBIGINT(lance_vane_default_index_cache_size_bytes()),
      SetLanceVaneIndexCacheSize, SetScope::GLOBAL);
  config.AddExtensionOption(
      LANCE_VANE_METADATA_CACHE_SIZE_SETTING,
      "Maximum bytes retained by the process-local Lance metadata cache",
      LogicalType::UBIGINT,
      Value::UBIGINT(lance_vane_default_metadata_cache_size_bytes()),
      SetLanceVaneMetadataCacheSize, SetScope::GLOBAL);
}
#endif

shared_ptr<LanceSharedSessionEntry>
GetOrCreateLanceSharedSessionEntry(ClientContext &context,
                                   bool *out_cache_hit) {
  auto &cache = ObjectCache::GetObjectCache(context);
#ifdef LANCE_VANE_DISTRIBUTED
  auto index_cache_size_bytes =
      LanceVaneSessionCacheSize(context, LANCE_VANE_INDEX_CACHE_SIZE_SETTING,
                                lance_vane_default_index_cache_size_bytes());
  auto metadata_cache_size_bytes =
      LanceVaneSessionCacheSize(context, LANCE_VANE_METADATA_CACHE_SIZE_SETTING,
                                lance_vane_default_metadata_cache_size_bytes());
#endif
  auto existing =
      cache.Get<LanceSharedSessionEntry>(LANCE_SHARED_SESSION_CACHE_KEY);
  if (out_cache_hit) {
    *out_cache_hit = existing != nullptr;
  }
  auto entry = existing ? existing
                        : cache.GetOrCreate<LanceSharedSessionEntry>(
                              LANCE_SHARED_SESSION_CACHE_KEY
#ifdef LANCE_VANE_DISTRIBUTED
                              ,
                              index_cache_size_bytes, metadata_cache_size_bytes
#endif
                          );
  if (!entry || !entry->Handle()) {
    throw IOException("Failed to access shared Lance session");
  }
#ifdef LANCE_VANE_DISTRIBUTED
  if (entry->IndexCacheSizeBytes() != index_cache_size_bytes ||
      entry->MetadataCacheSizeBytes() != metadata_cache_size_bytes) {
    throw InvalidInputException("Lance Vane cache sizes must be consistent "
                                "across database connections");
  }
#endif
  return entry;
}

LanceSessionState::LanceSessionState(ClientContext &context)
    : shared_session(GetOrCreateLanceSharedSessionEntry(
          context, &shared_session_cache_hit)) {}

LanceSessionState::~LanceSessionState() = default;

void *LanceSessionState::Handle() const {
  return shared_session ? shared_session->Handle() : nullptr;
}

void LanceSessionState::WriteProfilingInformation(std::ostream &ss) {
  LanceSessionStats stats{};

  auto *session = Handle();
  if (session && lance_session_get_stats(session, &stats) == 0) {
    ss << "Lance Session Cache: scope=database_shared shared_session_id="
       << shared_session->Id()
       << " object_cache_hit=" << (shared_session_cache_hit ? "true" : "false")
       << " approx_num_items=" << stats.approx_num_items
       << " size_bytes=" << stats.size_bytes;
#ifdef LANCE_VANE_DISTRIBUTED
    LanceVaneSessionCacheStats cache_stats{};
    if (lance_vane_session_get_cache_stats(session, &cache_stats) == 0) {
      ss << " index_capacity_bytes=" << shared_session->IndexCacheSizeBytes()
         << " index_entries=" << cache_stats.index_num_entries
         << " index_size_bytes=" << cache_stats.index_size_bytes
         << " index_hits=" << cache_stats.index_hits
         << " index_misses=" << cache_stats.index_misses
         << " metadata_capacity_bytes="
         << shared_session->MetadataCacheSizeBytes()
         << " metadata_entries=" << cache_stats.metadata_num_entries
         << " metadata_size_bytes=" << cache_stats.metadata_size_bytes
         << " metadata_hits=" << cache_stats.metadata_hits
         << " metadata_misses=" << cache_stats.metadata_misses;
    }
#endif
    ss << "\n";
    return;
  }

  ss << "Lance Session Cache: scope=database_shared shared_session_id="
     << (shared_session ? shared_session->Id() : 0)
     << " object_cache_hit=" << (shared_session_cache_hit ? "true" : "false")
     << " unavailable\n";
}

shared_ptr<LanceSessionState>
GetOrCreateLanceSessionState(ClientContext &context) {
  return context.registered_state->GetOrCreate<LanceSessionState>(
      LANCE_SESSION_STATE_KEY, context);
}

void *LanceGetSessionHandle(ClientContext &context) {
  auto state = GetOrCreateLanceSessionState(context);
  if (!state || !state->Handle()) {
    throw IOException("Failed to access Lance session state");
  }
  return state->Handle();
}

} // namespace duckdb
