// Standalone PR2 feasibility experiment. This registers no production function
// or filesystem. Enable with -DLANCE_BLOB_ACCESS_PROBE=ON in either host build.
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "lance_ffi.hpp"

#include <future>
#include <iostream>
#include <stdexcept>

namespace duckdb {

static void Check(bool condition, const char *message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

// Link and invoke every common entry point through the C++ declarations. The
// Rust tests exercise successful values, ownership and error injection.
static void CheckCommonAbi() {
  void *access = nullptr;
  Check(lance_blob_access_create(nullptr, &access) == -1 && !access,
        "access ABI outputs");
  void *blob = nullptr;
  uint64_t size = 1;
  Check(lance_blob_open(nullptr, &blob, &size) == -1 && !blob && size == 0,
        "open ABI outputs");
  size_t read = 1;
  Check(lance_blob_read_at(nullptr, 0, nullptr, 0, &read) == -1 && read == 0,
        "read ABI outputs");
  Check(!lance_blob_open_binding_dataset(nullptr, nullptr), "binding ABI");
  Check(!lance_blob_snapshot_token(nullptr, nullptr), "snapshot ABI");
  Check(lance_dataset_version(nullptr) == 0, "common dataset version ABI");
  Check(!lance_dataset_generation_id(nullptr), "common generation ABI");
  Check(!lance_dataset_checkout_version(nullptr, 1), "common checkout ABI");
  lance_blob_free(nullptr);
  lance_blob_access_free(nullptr);
}

// The future owns the DatabaseInstance. No ClientContext, Connection, secret
// pointer or FileOpener is borrowed across the worker-thread boundary.
static void CheckWorker(shared_ptr<DatabaseInstance> database, bool disabled) {
  auto &filesystem = FileSystem::GetFileSystem(*database);
  Check(filesystem.IsDisabledForPath("/tmp/blob-allowed/file") == disabled,
        "local dispatch disable check");
  Check(filesystem.SubSystemIsDisabled("S3FileSystem") == disabled,
        "disabled remote provider check");
  if (disabled) {
    return;
  }
  auto &config = DBConfig::GetConfig(*database);
  Check(config.CanAccessFile("/tmp/blob-allowed/file",
                             FileType::FILE_TYPE_REGULAR),
        "allowed directory");
  Check(config.CanAccessFile("s3://bucket/first/object",
                             FileType::FILE_TYPE_REGULAR),
        "allowed exact path");
  Check(!config.CanAccessFile("/tmp/blob-denied/file",
                              FileType::FILE_TYPE_REGULAR),
        "denied directory");
  Check(!config.CanAccessFile("s3://bucket/second/object",
                              FileType::FILE_TYPE_REGULAR),
        "denied object");
  auto &manager = SecretManager::Get(*database);
  auto transaction = CatalogTransaction::GetSystemTransaction(*database);
  for (const auto &scope : {"first", "second"}) {
    auto match = manager.LookupSecret(
        transaction, string("s3://bucket/") + scope + "/object", "blob_probe");
    Check(match.HasMatch() && match.GetSecret().GetName() == scope,
          "native target-scoped secret lookup");
  }
}

static void RunProbe() {
  shared_ptr<DatabaseInstance> retained;
  {
    DuckDB database(nullptr);
    Connection connection(database);
    retained = database.instance;
    auto &manager = SecretManager::Get(*retained);
    SecretType type;
    type.name = "blob_probe";
    type.default_provider = "config";
    manager.RegisterSecretType(type);
    auto transaction = CatalogTransaction::GetSystemTransaction(*retained);
    for (const auto &scope : {"first", "second"}) {
      auto secret = make_uniq<KeyValueSecret>(
          vector<string>{string("s3://bucket/") + scope + "/"}, "blob_probe",
          "config", scope);
      manager.RegisterSecret(transaction, std::move(secret),
                             OnCreateConflict::ERROR_ON_CONFLICT,
                             SecretPersistType::TEMPORARY);
    }
    // Initialize lazy secret state before callbacks. Configuration stays fixed
    // during each worker group; this experiment does not establish concurrent
    // SQL SET safety for DuckDB's mutable allowed-path containers.
    manager.LookupSecret(transaction, "s3://bucket/first/object", "blob_probe");
    auto &config = DBConfig::GetConfig(*retained);
    config.SetOptionByName(
        "allowed_directories",
        Value::LIST(LogicalType::VARCHAR, {Value("/tmp/blob-allowed/")}));
    config.SetOptionByName(
        "allowed_paths",
        Value::LIST(LogicalType::VARCHAR, {Value("s3://bucket/first/object")}));
    config.SetOptionByName("enable_external_access", Value::BOOLEAN(false));
    auto first = std::async(std::launch::async, CheckWorker, retained, false);
    auto second = std::async(std::launch::async, CheckWorker, retained, false);
    first.get();
    second.get();
  }
  // Both the initiating connection and DuckDB wrapper have been destroyed.
  std::async(std::launch::async, CheckWorker, retained, false).get();
  FileSystem::GetFileSystem(*retained).SetDisabledFileSystems(
      {"LocalFileSystem", "S3FileSystem"});
  std::async(std::launch::async, CheckWorker, retained, true).get();
}

} // namespace duckdb

int main() {
  try {
    duckdb::CheckCommonAbi();
    duckdb::RunProbe();
    std::cout << "Blob native access probe passed: worker policy, scoped "
                 "secrets, disabled filesystems, teardown\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
