#include "lance_blob_access.hpp"

#include "lance_common.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <atomic>

namespace duckdb {

string LanceNormalizeBlobTarget(const string &uri, bool local_path) {
  if (uri.find('\0') != string::npos) {
    throw InvalidInputException("Invalid Blob target");
  }
  auto normalized = lance_blob_normalize_uri(uri.c_str(), local_path);
  if (!normalized) {
    LanceConsumeLastError();
    throw InvalidInputException("Invalid or credential-bearing Blob target");
  }
  string result(normalized);
  lance_free_string(normalized);
  return result;
}

[[noreturn]] void LanceThrowBlobError(const char *operation) {
  auto code = lance_last_error_code();
  // Never forward provider diagnostics or option values, even for future FFI
  // error categories that have not yet adopted the Blob reader's redaction.
  LanceConsumeLastError();
  if (code == LANCE_BLOB_ERROR_ACCESS) {
    throw PermissionException("Lance Blob %s: backing access denied",
                              operation);
  }
  if (code == LANCE_BLOB_ERROR_RANGE) {
    throw InvalidInputException("Lance Blob %s: invalid byte range", operation);
  }
  if (code == LANCE_BLOB_ERROR_SNAPSHOT) {
    throw IOException(
        "Lance Blob %s: snapshot unavailable or identity mismatch", operation);
  }
  if (code == LANCE_BLOB_ERROR_NOT_FOUND) {
    throw IOException("Lance Blob %s: row, value or payload not found",
                      operation);
  }
  throw IOException("Lance Blob %s failed (code=%d)", operation, code);
}

static bool IsDisabledBlobFileSystem(FileSystem &filesystem,
                                     const string &path) {
  if (!filesystem.IsDisabledForPath(path)) {
    return false;
  }
  if (FileSystem::CreateLocal()->IsPathAbsolute(path) ||
      !filesystem.SubSystemIsDisabled("LocalFileSystem")) {
    return true;
  }
  for (const auto &name : filesystem.ListSubSystems()) {
    if (name == "LocalFileSystem") {
      // A registered handler using the default's name makes the denial
      // ambiguous. Never exempt such a handler from its disable setting.
      return true;
    }
  }
  // The public dispatcher API does not expose the selected handler. Asking
  // for its path separator dispatches without I/O or extension autoloading
  // and reports the disabled handler's name. Exempt only the exact native
  // default-local denial for remote paths; every other denial fails closed.
  try {
    filesystem.PathSeparator(path);
  } catch (const PermissionException &error) {
    return ErrorData(error).RawMessage() !=
           "File system LocalFileSystem has been disabled by configuration";
  }
  return true;
}

void LanceCheckBlobAccess(DatabaseInstance &database, const string &target,
                          uint32_t operation) {
  const bool synthetic = StringUtil::StartsWith(target, "lance-blob://");
  auto path = synthetic ? target : LanceNormalizeBlobTarget(target, true);
  auto &config = DBConfig::GetConfig(database);
  // Use the native policy API. DuckDB forbids editing allowed lists after
  // external access is disabled; configuration changes must follow the same
  // ordering as native file opens. The registry uses its own atomic snapshot.
  auto type = operation == LANCE_BLOB_PROVIDER || operation == LANCE_BLOB_LIST
                  ? FileType::FILE_TYPE_DIR
                  : FileType::FILE_TYPE_REGULAR;
  if (!config.CanAccessFile(path, type)) {
    throw PermissionException("Lance Blob backing access denied");
  }
  auto &filesystem = FileSystem::GetFileSystem(database);
  // Preserve native dispatch and custom handler priorities, but do not apply
  // the default-local fallback to Rust-backed remote targets.
  if (IsDisabledBlobFileSystem(filesystem, path)) {
    throw PermissionException("Lance Blob filesystem is disabled");
  }
  // Rust does not require DuckDB's cloud extensions to be loaded. Preserve
  // their native disable names even when no corresponding dispatcher exists.
  const char *backing = nullptr;
  if (synthetic) {
    backing = "LanceBlobFileSystem";
  } else if (StringUtil::StartsWith(path, "s3://") ||
             StringUtil::StartsWith(path, "gs://")) {
    backing = "S3FileSystem";
  } else if (StringUtil::StartsWith(path, "hf://")) {
    backing = "HuggingFaceFileSystem";
  } else if (StringUtil::StartsWith(path, "az://")) {
    backing = "AzureBlobStorageFileSystem";
  }
  if (backing && filesystem.SubSystemIsDisabled(backing)) {
    throw PermissionException("Lance Blob backing filesystem is disabled");
  }
}

// Keep the native transaction's visibility while it is active, without lending
// a ClientContext or Transaction pointer to Rust workers. Native lifecycle
// callbacks retire the view before transaction storage can be reclaimed. The
// state can outlive its connection; subsequent lookups use committed secrets.
class BlobSecretState final : public ClientContextState {
public:
  void Capture(ClientContext &context) {
    unique_ptr<CatalogTransaction> captured;
    if (context.transaction.HasActiveTransaction()) {
      auto transaction =
          CatalogTransaction::GetSystemCatalogTransaction(context);
      captured = make_uniq<CatalogTransaction>(
          DatabaseInstance::GetDatabase(context), transaction.transaction_id,
          transaction.start_time);
    }
    lock_guard<mutex> guard(lock);
    transaction = std::move(captured);
  }

  void QueryBegin(ClientContext &context) override { Capture(context); }
  void TransactionCommit(MetaTransaction &, ClientContext &) override {
    Reset();
  }
  void TransactionRollback(MetaTransaction &, ClientContext &) override {
    Reset();
  }

  void Resolve(DatabaseInstance &database, const string &path,
               vector<string> &keys, vector<string> &values) {
    lock_guard<mutex> guard(lock);
    LanceFillStorageOptionsFromSecrets(
        database,
        transaction ? *transaction
                    : CatalogTransaction::GetSystemTransaction(database),
        path, keys, values);
  }

private:
  void Reset() {
    lock_guard<mutex> guard(lock);
    transaction.reset();
  }

  mutex lock;
  unique_ptr<CatalogTransaction> transaction;
};

struct BlobAccessState {
  explicit BlobAccessState(ClientContext &context)
      : database(DatabaseInstance::GetDatabase(context).shared_from_this()) {
    // Initialize lazy secret storage on the initiating thread. Subsequent
    // lookups use SecretManager's own storage synchronization and owned
    // matches.
    auto transaction =
        context.transaction.HasActiveTransaction()
            ? CatalogTransaction::GetSystemCatalogTransaction(context)
            : CatalogTransaction::GetSystemTransaction(*database);
    SecretManager::Get(*database).LookupSecret(transaction, "", "lance");
    secrets = context.registered_state->GetOrCreate<BlobSecretState>(
        "lance_blob_secret_transaction");
    secrets->Capture(context);
#ifdef LANCE_VANE_DISTRIBUTED
    try {
      LanceCaptureBlobSessionOptions(context, session_keys, session_values);
    } catch (const InvalidInputException &) {
      // Invalid S3 settings matter only if a target needs this fallback. A
      // local/HF target or an explicit scoped secret must remain independent.
      session_options_valid = false;
      session_keys.clear();
      session_values.clear();
    }
#endif
  }

  std::atomic<uint64_t> references{1};
  shared_ptr<DatabaseInstance> database;
  shared_ptr<BlobSecretState> secrets;
#ifdef LANCE_VANE_DISTRIBUTED
  vector<string> session_keys;
  vector<string> session_values;
  bool session_options_valid = true;
#endif
};

struct BlobAccessResultOwner {
  string target;
  vector<string> keys;
  vector<string> values;
  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
};

static void RetainAccess(void *context) {
  static_cast<BlobAccessState *>(context)->references.fetch_add(
      1, std::memory_order_relaxed);
}

static void ReleaseAccess(void *context) {
  auto state = static_cast<BlobAccessState *>(context);
  if (state->references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    unique_ptr<BlobAccessState> owner(state);
  }
}

static int32_t ResolveAccess(void *context, const char *target,
                             uint32_t operation, bool resolve_options,
                             LanceBlobAccessResult *result) {
  *result = {};
  try {
    auto &state = *static_cast<BlobAccessState *>(context);
    if (!target || operation < LANCE_BLOB_PROVIDER ||
        operation > LANCE_BLOB_READ) {
      return -1;
    }
    LanceCheckBlobAccess(*state.database, target, operation);
    auto owner = make_uniq<BlobAccessResultOwner>();
    owner->target = target;
    auto lookup_path = LanceNormalizeBlobTarget(target, true);
    // Metadata/range callbacks only authorize. Resolve options at provider
    // boundaries, using the native scoped lookup rather than copied matchers.
    if (operation == LANCE_BLOB_PROVIDER && resolve_options) {
      state.secrets->Resolve(*state.database, lookup_path, owner->keys,
                             owner->values);
#ifdef LANCE_VANE_DISTRIBUTED
      if (StringUtil::StartsWith(lookup_path, "s3://")) {
        CanonicalizeLanceS3StorageOptions(owner->keys, owner->values);
        if (owner->keys.empty()) {
          if (!state.session_options_valid) {
            return -1;
          }
          owner->keys = state.session_keys;
          owner->values = state.session_values;
        }
      }
#endif
    }
    BuildStorageOptionPointerArrays(owner->keys, owner->values, owner->key_ptrs,
                                    owner->value_ptrs);
    result->normalized_uri = owner->target.c_str();
    result->option_keys = owner->key_ptrs.data();
    result->option_values = owner->value_ptrs.data();
    result->option_count = owner->keys.size();
    result->result_owner = owner.release();
    return 0;
  } catch (...) {
    // No C++ exception or unfiltered diagnostic crosses the C ABI.
    return -1;
  }
}

static void ReleaseResult(void *, LanceBlobAccessResult *result) {
  unique_ptr<BlobAccessResultOwner> owner(
      static_cast<BlobAccessResultOwner *>(result->result_owner));
  *result = {};
}

LanceBlobAccess::LanceBlobAccess(ClientContext &context) {
  auto state = make_uniq<BlobAccessState>(context);
  LanceBlobAccessCallbacks callbacks{state.get(), RetainAccess, ReleaseAccess,
                                     ResolveAccess, ReleaseResult};
  if (lance_blob_access_create(&callbacks, &access) != 0) {
    LanceThrowBlobError("create access context");
  }
  ReleaseAccess(state.release());
}

LanceBlobAccess::~LanceBlobAccess() { lance_blob_access_free(access); }

} // namespace duckdb
