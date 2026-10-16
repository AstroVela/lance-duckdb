#include "duckdb.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "core_functions_extension.hpp"
#include "lance_blob.hpp"
#include "lance_blob_access.hpp"
#include "lance_blob_filesystem.hpp"
#include "lance_common.hpp"
#include "lance_dataset_cache.hpp"
#include "lance_extension.hpp"

#include <future>
#include <functional>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace duckdb {

static void Check(bool condition, const string &message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

template <class FUNCTION>
static void Throws(FUNCTION function, const string &message) {
  bool threw = false;
  try {
    function();
  } catch (const std::exception &) {
    threw = true;
  }
  Check(threw, message);
}

static string Quote(const string &value) {
  string result = "'";
  for (auto byte : value) {
    result += byte;
    if (byte == '\'') {
      result += '\'';
    }
  }
  return result + "'";
}

static unique_ptr<MaterializedQueryResult> Query(Connection &connection,
                                                 const string &sql) {
  auto result = connection.Query(sql);
  if (result->HasError()) {
    throw std::runtime_error(result->GetError());
  }
  return result;
}

static void Load(DuckDB &database) {
  database.LoadStaticExtension<CoreFunctionsExtension>();
  database.LoadStaticExtension<LanceExtension>();
}

static void CheckContextSecretOptions(Connection &connection,
                                      const string &target,
                                      const string &expected) {
  string open_path;
  vector<string> keys, values;
  ResolveLanceStorageOptions(*connection.context, target, open_path, keys,
                             values);
  Check(keys == vector<string>{"hf_token"} &&
            values == vector<string>{expected},
        "scan resolves scoped options for " + target);
#ifdef LANCE_VANE_DISTRIBUTED
  Check(LanceHasMatchingStorageSecret(*connection.context, target),
        "distributed dependency detection agrees with resolution");
#endif
}

static void CheckSecretOptions(Connection &connection, const string &target,
                               const string &expected) {
  connection.BeginTransaction();
  CheckContextSecretOptions(connection, target, expected);
  connection.Commit();
  vector<string> keys, values;
  LanceFillStorageOptionsFromSecrets(
      DatabaseInstance::GetDatabase(*connection.context),
      CatalogTransaction::GetSystemTransaction(
          DatabaseInstance::GetDatabase(*connection.context)),
      LanceNormalizeBlobTarget(LanceNormalizeS3Scheme(target), true), keys,
      values);
  Check(keys == vector<string>{"hf_token"} &&
            values == vector<string>{expected},
        "committed lookup resolves the same scoped options for " + target);
}

static void CheckSecretScope(const string &directory, const string &scope,
                             const string &target, const string &outside,
                             bool persisted) {
  {
    DuckDB database(nullptr);
    Load(database);
    Connection connection(database);
    Query(connection, "SET secret_directory=" + Quote(directory));
    const string create =
        persisted ? "CREATE PERSISTENT SECRET " : "CREATE SECRET ";
    Query(connection, create + "fallback (TYPE LANCE, HF_TOKEN 'fallback')");
    Query(connection, create + "scoped (TYPE LANCE, SCOPE " + Quote(scope) +
                          ", HF_TOKEN 'scoped')");
    CheckSecretOptions(connection, target, "scoped");
    CheckSecretOptions(connection, outside, "fallback");
    if (!persisted) {
      Query(connection, "DROP SECRET fallback");
      CheckSecretOptions(connection, target, "scoped");
      return;
    }
  }
  DuckDB reopened(nullptr);
  Load(reopened);
  Connection connection(reopened);
  Query(connection, "SET secret_directory=" + Quote(directory));
  CheckSecretOptions(connection, target, "scoped");
  CheckSecretOptions(connection, outside, "fallback");
  Query(connection, "DROP PERSISTENT SECRET fallback");
  CheckSecretOptions(connection, target, "scoped");
  Query(connection, "CREATE SECRET temporary_scoped (TYPE LANCE, SCOPE " +
                        Quote(scope) + ", HF_TOKEN 'temporary')");
  CheckSecretOptions(connection, target, "temporary");
  Query(connection, "DROP SECRET temporary_scoped");
  CheckSecretOptions(connection, target, "scoped");
}

static void CheckSecretTransactions() {
  DuckDB database(nullptr);
  Load(database);
  Connection connection(database), observer(database);
  const string target = "s3://bucket/Some%20Folder/table.lance";
  Query(connection, "CREATE SECRET fallback (TYPE LANCE, HF_TOKEN 'fallback')");
  Query(connection, "CREATE SECRET scoped (TYPE LANCE, "
                    "SCOPE 'S3A://bucket/Some Folder/', HF_TOKEN 'original')");
  connection.BeginTransaction();
  Query(connection,
        "CREATE OR REPLACE SECRET scoped (TYPE LANCE, "
        "SCOPE 's3n://bucket/Some%20Folder/', HF_TOKEN 'replaced')");
  CheckContextSecretOptions(connection, target, "replaced");
  connection.Rollback();
  CheckSecretOptions(connection, target, "original");

  connection.BeginTransaction();
  Query(connection, "DROP SECRET scoped");
  CheckContextSecretOptions(connection, target, "fallback");
  connection.Rollback();
  CheckSecretOptions(connection, target, "original");

  connection.BeginTransaction();
  Query(connection,
        "CREATE OR REPLACE SECRET scoped (TYPE LANCE, "
        "SCOPE 's3://bucket/Some%20Folder/', HF_TOKEN 'committed')");
  connection.Commit();
  CheckSecretOptions(observer, target, "committed");
  Query(connection, "DROP SECRET scoped");
  CheckSecretOptions(observer, target, "fallback");
}

static void CheckRemoteSubsystemPolicy() {
  DuckDB database(nullptr);
  Load(database);
  Connection connection(database);
  auto &instance = DatabaseInstance::GetDatabase(*connection.context);
  Query(connection, "SET disabled_filesystems='LocalFileSystem'");
  for (const auto &entry : vector<pair<string, string>>{
           {"s3://bucket/table.lance", "S3FileSystem"},
           {"gs://bucket/table.lance", "S3FileSystem"},
           {"hf://datasets/owner/repo/table.lance", "HuggingFaceFileSystem"},
           {"az://container/table.lance", "AzureBlobStorageFileSystem"},
           {"oss://bucket/table.lance", ""},
           {"lance-blob://v1/", "LanceBlobFileSystem"}}) {
    for (auto operation : {LANCE_BLOB_PROVIDER, LANCE_BLOB_READ}) {
      LanceCheckBlobAccess(instance, entry.first, operation);
    }
    if (entry.second.empty()) {
      continue;
    }
    DuckDB disabled(nullptr);
    Load(disabled);
    Connection denied(disabled);
    Query(denied, "SET disabled_filesystems=" + Quote(entry.second));
    Throws(
        [&] {
          LanceCheckBlobAccess(DatabaseInstance::GetDatabase(*denied.context),
                               entry.first, LANCE_BLOB_PROVIDER);
        },
        "explicitly disabled remote subsystem is rejected");
  }
  Throws(
      [&] { LanceCheckBlobAccess(instance, "test/data", LANCE_BLOB_PROVIDER); },
      "local targets still honor LocalFileSystem disable");
}

class TestRemoteFileSystem : public FileSystem {
public:
  TestRemoteFileSystem(string name, string prefix, bool manual = false)
      : name(std::move(name)), prefix(std::move(prefix)), manual(manual) {}

  string GetName() const override { return name; }
  bool CanHandleFile(const string &path) override {
    return path.compare(0, prefix.size(), prefix) == 0;
  }
  bool IsManuallySet() override { return manual; }

private:
  string name;
  string prefix;
  bool manual;
};

static void CheckRegisteredRemotePolicy() {
  for (bool disable_local : {false, true}) {
    for (bool manual : {false, true}) {
      DuckDB database(nullptr);
      Load(database);
      Connection connection(database);
      auto &instance = DatabaseInstance::GetDatabase(*connection.context);
      auto &filesystem = FileSystem::GetFileSystem(instance);
      filesystem.RegisterSubSystem(
          make_uniq<TestRemoteFileSystem>("CustomS3FS", "s3://", manual));
      filesystem.RegisterSubSystem(
          make_uniq<TestRemoteFileSystem>("OtherS3FS", "s3://other/"));
      Query(connection, "SET disabled_filesystems=" +
                            Quote(disable_local ? "LocalFileSystem,CustomS3FS"
                                                : "CustomS3FS"));
      Check(filesystem.IsDisabledForPath("s3://bucket/table.lance"),
            "native dispatch detects the disabled custom handler");
      for (auto operation : {LANCE_BLOB_PROVIDER, LANCE_BLOB_READ}) {
        Throws(
            [&] {
              LanceCheckBlobAccess(instance, "s3://bucket/table.lance",
                                   operation);
            },
            "Blob access preserves the registered handler's disable policy");
        if (manual) {
          Throws(
              [&] {
                LanceCheckBlobAccess(instance, "s3://other/table.lance",
                                     operation);
              },
              "manual handler retains native dispatch priority");
        } else {
          LanceCheckBlobAccess(instance, "s3://other/table.lance", operation);
        }
        LanceCheckBlobAccess(instance, "az://container/table.lance", operation);
      }
    }
  }
  DuckDB database(nullptr);
  Load(database);
  Connection connection(database);
  auto &instance = DatabaseInstance::GetDatabase(*connection.context);
  FileSystem::GetFileSystem(instance).RegisterSubSystem(
      make_uniq<TestRemoteFileSystem>("LocalFileSystem", "s3://"));
  Query(connection, "SET disabled_filesystems='LocalFileSystem'");
  Throws(
      [&] {
        LanceCheckBlobAccess(instance, "s3://bucket/table.lance",
                             LANCE_BLOB_READ);
      },
      "a registered handler sharing the default's name is not exempt");
}

#ifdef LANCE_VANE_DISTRIBUTED
static void CheckSecretPlanDependency() {
  DuckDB database(nullptr);
  Load(database);
  Connection connection(database);
  const string relative = "test/data/test_data.lance";
  const auto absolute = LanceNormalizeDatasetPath(relative);
  // A control plan must serialize successfully without coordinator secrets.
  auto control = connection.ExtractPlan("SELECT id FROM " + Quote(relative));
  MemoryStream control_stream;
  BinarySerializer::Serialize(*control, control_stream);
  for (const auto &path :
       {relative, absolute, LanceNormalizeBlobTarget(absolute)}) {
    Query(connection, "CREATE SECRET scoped (TYPE LANCE, SCOPE " +
                          Quote(absolute) + ", HF_TOKEN 'coordinator-value')");
    auto plan = connection.ExtractPlan("SELECT id FROM " + Quote(path));
    Query(connection, "DROP SECRET scoped");
    bool rejected = false;
    try {
      MemoryStream stream;
      BinarySerializer::Serialize(*plan, stream);
    } catch (const NotImplementedException &error) {
      Check(string(error.what()).find("coordinator-only TYPE LANCE secret") !=
                string::npos,
            "serialization fails for the secret dependency");
      rejected = true;
    }
    Check(rejected,
          "serialized scan retains its coordinator secret dependency");
  }
}

static void CheckFailedBindCache(const string &dataset) {
  DuckDB database(nullptr);
  Load(database);
  Connection reader(database), writer(database);
  const auto path = dataset + ".failed-bind.lance";
  for (const auto &column : {"missing", "id"}) {
    Query(writer, "COPY (SELECT 1 AS id) TO " + Quote(path) +
                      " (FORMAT LANCE, MODE 'overwrite')");
    auto failed = reader.Query("SELECT lance_blob_uri(" + Quote(path) + ", " +
                               Quote(column) + ", 0)");
    Check(failed->HasError() &&
              failed->GetError().find("Blob v2 column") != string::npos,
          "Blob column validation fails after opening the dataset");
    Query(writer, "COPY (SELECT 2 AS id) TO " + Quote(path) +
                      " (FORMAT LANCE, MODE 'append')");
    auto rows = Query(reader, "SELECT count(*) FROM " + Quote(path));
    Check(rows->GetValue(0, 0).GetValue<int64_t>() == 2,
          "failed Blob bind expires the cached dataset before the next query");
  }
}
#endif

static void CheckAccessRegressions(const string &dataset) {
  idx_t failures = 0;
  auto run = [&](const string &name, const std::function<void()> &test) {
    try {
      test();
    } catch (const std::exception &error) {
      std::cerr << name << ": " << error.what() << '\n';
      failures++;
    }
  };
  for (bool persisted : {false, true}) {
    auto suffix = persisted ? "persisted" : "temporary";
    run(string("S3 scope ") + suffix, [&] {
      CheckSecretScope(dataset + ".secrets-s3", "S3A://bucket/Some Folder/",
                       "s3n://bucket/Some%20Folder/table.lance",
                       "s3://bucket/Some%20Folder-other/table.lance",
                       persisted);
    });
    run(string("encoded S3 scope ") + suffix, [&] {
      CheckSecretScope(dataset + ".secrets-s3-slash",
                       "s3://bucket/Some%2FFolder%2f",
                       "s3://bucket/Some/Folder/table.lance",
                       "s3://bucket/Some/Folder-other/table.lance", persisted);
    });
    run(string("encoded HF scope ") + suffix, [&] {
      CheckSecretScope(dataset + ".secrets-hf-slash",
                       "hf://dataset/Owner/Repo/Some%2FFolder/",
                       "hf://datasets/owner/repo/Some/Folder/table.lance",
                       "hf://datasets/owner/repo/Some/Folder-other/table.lance",
                       persisted);
    });
    run(string("HF scope ") + suffix, [&] {
      CheckSecretScope(dataset + ".secrets-hf", "HF://dataset/Owner/Repo/Case/",
                       "hf://datasets/OWNER/REPO/Case/table.lance",
                       "hf://datasets/owner/repo/case/table.lance", persisted);
    });
    run(string("local scope ") + suffix, [&] {
      CheckSecretScope(dataset + ".secrets-local", "test/data/",
                       "test/./data/test_data.lance",
                       "test/data-other/test_data.lance", persisted);
    });
    run(string("encoded local scope ") + suffix, [&] {
      CheckSecretScope(
          dataset + ".secrets-encoded-local",
          LanceNormalizeBlobTarget(FileSystem::GetWorkingDirectory()) +
              "/test/data%2F",
          "test/data/test_data.lance", "test/data-other/test_data.lance",
          persisted);
    });
    run(string("scheme scope ") + suffix, [&] {
      CheckSecretScope(dataset + ".secrets-scheme", "S3A://",
                       "s3://bucket/table.lance", "gs://bucket/table.lance",
                       persisted);
    });
  }
  run("remote filesystem policy", CheckRemoteSubsystemPolicy);
  run("registered remote filesystem policy", CheckRegisteredRemotePolicy);
  run("secret transactions", CheckSecretTransactions);
#ifdef LANCE_VANE_DISTRIBUTED
  run("distributed secret dependency", CheckSecretPlanDependency);
  run("failed Blob bind cache", [&] { CheckFailedBindCache(dataset); });
#endif
  Check(failures == 0,
        "Blob access regressions: " + to_string(failures) + " failed");
}

static string RouteSQL(const string &dataset, const string &column = "video") {
  return "SELECT lance_blob_uri(" + Quote(dataset) + ", " + Quote(column) +
         ", _rowid) FROM " + Quote(dataset) + " ORDER BY id";
}

static vector<string> Routes(Connection &connection, const string &dataset,
                             const string &column = "video") {
  auto result = Query(connection, RouteSQL(dataset, column));
  vector<string> paths;
  for (idx_t row = 0; row < result->RowCount(); row++) {
    paths.push_back(result->GetValue(0, row).GetValue<string>());
  }
  return paths;
}

static vector<string> Expected() {
  vector<string> values{"abc",
                        "",
                        "",
                        "",
                        "external-slice-data!",
                        "PREFIX!external-slice-data!SUFFIX",
                        ""};
  for (idx_t i = 0; i < 64; i++) {
    values[1] += char(i);
  }
  for (idx_t i = 64; i < 144; i++) {
    values[2] += char(i);
  }
  for (idx_t i = 0; i < 2 * 1024 * 1024 + 257; i++) {
    values[3] += char(i % 251);
  }
  return values;
}

static void CheckUriParser(const string &path) {
  for (const auto &aliases : vector<pair<string, string>>{
           {"HF://dataset/Owner/Repo/Some%20File.lance",
            "hf://datasets/owner/repo/Some File.lance"},
           {"S3A://bucket/Some File.lance", "s3://bucket/Some%20File.lance"},
           {"s3://bucket/Some%2FFolder/table.lance",
            "s3://bucket/Some/Folder/table.lance"},
           {"hf://datasets/Owner/Repo/Some%2fFolder/table.lance",
            "hf://datasets/owner/repo/Some/Folder/table.lance"},
           {"s3://bucket/%53ome%252FFolder/table.lance",
            "s3://bucket/Some%252FFolder/table.lance"}}) {
    Check(LanceBuildResolvedPathDatasetCacheKey(aliases.first, {}, {}) ==
              LanceBuildResolvedPathDatasetCacheKey(aliases.second, {}, {}),
          "remote aliases share the scan and URI cache identity");
  }
  Check(LanceBuildResolvedPathDatasetCacheKey(
            "s3://bucket/Some%252FFolder/table.lance", {}, {}) !=
            LanceBuildResolvedPathDatasetCacheKey(
                "s3://bucket/Some%2FFolder/table.lance", {}, {}),
        "literal percent escapes remain distinct from path separators");
  auto parsed = ParseLanceBlobUri(path);
  parsed.column = "info.\"clip.with.dot +%&?中文\"";
  parsed.row_id = std::numeric_limits<uint64_t>::max();
  auto decoded = ParseLanceBlobUri(EncodeLanceBlobUri(parsed));
  Check(decoded.column == parsed.column && decoded.row_id == parsed.row_id,
        "URI UTF8/uint64 roundtrip");
  Check(decoded.dataset == parsed.dataset, "dataset roundtrip");
  for (const auto &invalid :
       {"lance-blob://v2/?", "lance-blob://v1/x?", "lance-blob://v1:80/?"}) {
    Throws([&] { ParseLanceBlobUri(invalid); }, "bad authority/path");
  }
  for (const auto &suffix : {"&unknown=1", "&rowid=1", "&", "#fragment"}) {
    Throws([&] { ParseLanceBlobUri(path + suffix); },
           "invalid extra URI field");
  }
  for (const auto &field :
       {"dataset", "column", "version", "rowid", "snapshot"}) {
    auto key = string(field) + "=";
    auto start = path.find(key);
    auto end = path.find('&', start);
    auto count = end == string::npos ? string::npos : end - start;
    for (const auto &value :
         {"", "%", "%GG", "%00", "%FF", "+1", "-1", "18446744073709551616"}) {
      // A numeric string is a valid column; numeric limits apply to numeric
      // fields.
      if ((string(field) == "dataset" || string(field) == "column") &&
          (string(value) == "-1" || string(value) == "18446744073709551616")) {
        continue;
      }
      auto invalid = path;
      invalid.replace(start, count, key + value);
      Throws([&] { ParseLanceBlobUri(invalid); },
             "invalid URI value: " + string(field) + "/" + value);
    }
  }
  Throws([&] { ParseLanceBlobUri(path + string(65536, 'x')); },
         "oversized URI");
  parsed.column = "%26rowid=5";
  Check(ParseLanceBlobUri(EncodeLanceBlobUri(parsed)).column == parsed.column,
        "decode once");
}

static void CheckFile(Connection &connection, const string &path,
                      const string &expected) {
  auto &filesystem = FileSystem::GetFileSystem(*connection.context);
  auto file = filesystem.OpenFile(path, FileFlags::FILE_FLAGS_READ);
  Check(file->CanSeek() && !file->OnDiskFile() && file->SeekPosition() == 0,
        "seekable virtual file");
  Check(file->GetFileSize() == expected.size() &&
            file->GetType() == FileType::FILE_TYPE_REGULAR,
        "blob metadata");
  Check(file->file_system.GetLastModifiedTime(*file) == timestamp_t(0),
        "unknown modification time");
  Check(file->file_system.GetVersionTag(*file).empty(), "no payload ETag");
  string buffer(expected.size() + 19, '\0');
  Check(file->Read(&buffer[0], buffer.size()) == int64_t(expected.size()),
        "clipped sequential read");
  Check(buffer.substr(0, expected.size()) == expected, "whole payload bytes");
  Check(file->Read(&buffer[0], 1) == 0 && file->Read(&buffer[0], 1) == 0,
        "repeated EOF");
  file->Read(nullptr, 0, expected.size());
  file->Seek(0);
  if (!expected.empty()) {
    auto first = MinValue<idx_t>(7, expected.size());
    Check(file->Read(&buffer[0], first) == int64_t(first),
          "small sequential read");
    auto position = file->SeekPosition();
    file->Read(&buffer[0], first, expected.size() - first);
    Check(buffer.substr(0, first) == expected.substr(expected.size() - first),
          "positional tail");
    Check(file->SeekPosition() == position, "positional read preserves cursor");
    file->Seek(1);
    file->Seek(0);
    Check(file->Read(&buffer[0], first) == int64_t(first) &&
              buffer.substr(0, first) == expected.substr(0, first),
          "backward seek and cached read");
    auto second = filesystem.OpenFile(path, FileFlags::FILE_FLAGS_READ);
    Check(second->SeekPosition() == 0 && file->SeekPosition() == first,
          "independent cursor");
  }
  auto position = file->SeekPosition();
  Throws([&] { file->Seek(expected.size() + 1); }, "seek beyond EOF");
  Throws([&] { file->Read(&buffer[0], 1, expected.size()); },
         "positional beyond EOF");
  Throws([&] { file->file_system.Read(*file, &buffer[0], -1); },
         "negative sequential length");
  Throws([&] { file->file_system.Read(*file, &buffer[0], -1, 0); },
         "negative positional length");
  Check(file->SeekPosition() == position, "failed reads preserve cursor");
  Throws([&] { file->Write(&buffer[0], 1); }, "write rejection");
  Throws([&] { file->Truncate(0); }, "truncate rejection");
  file->Close();
  file->Close();
  Throws([&] { file->Read(&buffer[0], 1); }, "read after close");
  Throws([&] { file->Seek(0); }, "seek after close");
  Throws([&] { file->GetFileSize(); }, "size after close");

  auto statement = connection.Prepare("SELECT content FROM read_blob(?)");
  vector<Value> parameters{Value(path)};
  auto result = statement->Execute(parameters, false);
  if (result->HasError()) {
    throw std::runtime_error(result->GetError());
  }
  auto value = result->Fetch()->GetValue(0, 0);
  Check(StringValue::Get(value) == expected, "SQL read_blob payload");
  Check(filesystem.FileExists(path), "FileExists on Blob");
  Check(filesystem.Glob(path).size() == 1, "literal URI glob");
}

static void CheckSql(Connection &connection, const string &dataset) {
  for (const auto &sql : vector<string>{
           "SELECT lance_blob_uri(NULL, 'video', NULL)",
           "SELECT lance_blob_uri(" + Quote(dataset) + ", NULL, NULL)",
           "SELECT lance_blob_uri(d, 'video', 0) FROM (VALUES (" +
               Quote(dataset) + ")) t(d)",
           "SELECT lance_blob_uri(" + Quote(dataset) +
               ", c, 0) FROM (VALUES ('video')) t(c)",
           "SELECT lance_blob_uri(" + Quote(dataset) + ", 'plain', 0)",
           "SELECT lance_blob_uri(" + Quote(dataset) + ", 'clips.item', 0)",
           "SELECT lance_blob_uri(" + Quote(dataset) + ", 'video.data', 0)"}) {
    Check(connection.Query(sql)->HasError(), "SQL constant/column validation");
  }
  auto null = Query(connection, "SELECT lance_blob_uri(" + Quote(dataset) +
                                    ", 'video', NULL)");
  Check(null->GetValue(0, 0).IsNull(), "null row ID");
  auto unsigned_rows =
      Query(connection, "SELECT lance_blob_uri(" + Quote(dataset) +
                            ", 'video', x) FROM (VALUES (0::UBIGINT), "
                            "(18446744073709551615::UBIGINT)) t(x)");
  Check(ParseLanceBlobUri(unsigned_rows->GetValue(0, 0).GetValue<string>())
                .row_id == 0,
        "row zero");
  Check(ParseLanceBlobUri(unsigned_rows->GetValue(0, 1).GetValue<string>())
                .row_id == std::numeric_limits<uint64_t>::max(),
        "unsigned max row ID");
  auto plan =
      connection.ExtractPlan("SELECT lance_blob_uri(" + Quote(dataset) +
                             ", 'video', i::UBIGINT) FROM range(1) t(i)");
  bool rejected = false;
  try {
    plan->Copy(*connection.context);
  } catch (const std::exception &error) {
    rejected =
        string(error.what()).find("cannot be serialized") != string::npos;
  }
  Check(rejected, "bound URI expressions cannot be serialized");
}

static void CheckFlagsAndErrors(Connection &connection,
                                const vector<string> &paths) {
  auto &filesystem = FileSystem::GetFileSystem(*connection.context);
  auto read = FileFlags::FILE_FLAGS_READ;
  for (auto flag :
       {FileFlags::FILE_FLAGS_WRITE, FileFlags::FILE_FLAGS_APPEND,
        FileFlags::FILE_FLAGS_FILE_CREATE,
        FileFlags::FILE_FLAGS_FILE_CREATE_NEW, FileFlags::FILE_FLAGS_PRIVATE,
        FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE,
        FileFlags::FILE_FLAGS_NULL_IF_EXISTS,
        FileFlags::FILE_FLAGS_DIRECT_IO}) {
    Throws([&] { filesystem.OpenFile(paths[0], read | flag); },
           "reject mutation/open hint");
  }
  LanceBlobFileSystem raw;
  Throws([&] { raw.OpenFile(paths[0], read); }, "context required");
  Check(!filesystem.FileExists(paths[7]), "null Blob absence");
  auto missing = ParseLanceBlobUri(paths[0]);
  missing.row_id = std::numeric_limits<uint64_t>::max();
  Check(!filesystem.FileExists(EncodeLanceBlobUri(missing)),
        "missing row absence");
  missing.row_id = 0;
  missing.snapshot = string(64, '0');
  Throws([&] { filesystem.FileExists(EncodeLanceBlobUri(missing)); },
         "snapshot error is not absence");
  Throws([&] { filesystem.FileExists("lance-blob://v9/?bad=1"); },
         "malformed URI is not absence");
}

static void CheckPolicy(const string &dataset, const string &external,
                        const vector<string> &paths) {
  for (idx_t mode = 0; mode < 5; mode++) {
    DuckDB database(nullptr);
    Load(database);
    Connection connection(database);
    // Initialize native persistent secret storage before restricting local I/O.
    Query(connection, "SELECT count(*) FROM duckdb_secrets()");
    auto allowed =
        mode == 0 ? "[]" : "[" + Quote(paths[0]) + "," + Quote(paths[4]) + "]";
    if (mode == 3) {
      allowed = "[" + Quote(paths[0]) + "," + Quote(paths[4]) + "," +
                Quote(external) + "]";
    }
    Query(connection, "SET allowed_paths=" + allowed);
    if (mode >= 2) {
      Query(connection,
            "SET allowed_directories=[" + Quote(dataset + "/") + "]");
    }
    Query(connection, "SET enable_external_access=false");
    if (mode == 4) {
      Query(connection, "SET disabled_filesystems='LocalFileSystem'");
    }
    auto &filesystem = FileSystem::GetFileSystem(*connection.context);
    if (mode < 2 || mode == 4) {
      Throws([&] { filesystem.OpenFile(paths[0], FileFlags::FILE_FLAGS_READ); },
             "synthetic/backing policy denial");
    } else {
      auto file = filesystem.OpenFile(paths[0], FileFlags::FILE_FLAGS_READ);
      char buffer[3];
      Check(file->Read(buffer, 3) == 3 && string(buffer, 3) == "abc",
            "native allowed directory exception");
    }
    if (mode == 3) {
      auto file = filesystem.OpenFile(paths[4], FileFlags::FILE_FLAGS_READ);
      Check(file->GetFileSize() == Expected()[4].size(),
            "external exact allowed path");
    } else {
      Throws([&] { filesystem.FileExists(paths[4]); },
             "external denial stays explicit");
    }
  }
  DuckDB database(nullptr);
  Load(database);
  Connection connection(database);
  Query(connection, "SET disabled_filesystems='LanceBlobFileSystem'");
  Throws(
      [&] {
        FileSystem::GetFileSystem(*connection.context)
            .OpenFile(paths[0], FileFlags::FILE_FLAGS_READ);
      },
      "disabled virtual filesystem");
}

static void CheckLifetimeAndConcurrency(const string &path,
                                        const string &expected) {
  unique_ptr<FileHandle> retained;
  {
    DuckDB database(nullptr);
    Load(database);
    Connection connection(database);
    retained = FileSystem::GetFileSystem(*connection.context)
                   .OpenFile(path, FileFlags::FILE_FLAGS_READ);
  }
  auto future = std::async(std::launch::async, [&] {
    string buffer(64, '\0');
    retained->Read(&buffer[0], buffer.size(), expected.size() - buffer.size());
    Check(buffer == expected.substr(expected.size() - buffer.size()),
          "owned context after connection destruction");
  });
  future.get();
  retained->Close();
  char closed_byte;
  Throws([&] { retained->Read(&closed_byte, 1); },
         "closed handle retains filesystem lifetime");
  Throws([&] { retained->GetFileSize(); },
         "closed metadata retains filesystem lifetime");
  DuckDB database(nullptr);
  Load(database);
  Connection connection(database);
  vector<unique_ptr<FileHandle>> files;
  vector<std::future<void>> reads;
  for (idx_t i = 0; i < 4; i++) {
    files.push_back(FileSystem::GetFileSystem(*connection.context)
                        .OpenFile(path, FileFlags::FILE_FLAGS_READ));
  }
  for (idx_t i = 0; i < files.size(); i++) {
    reads.push_back(std::async(std::launch::async, [&, i] {
      for (idx_t offset = i; offset + 31 < expected.size(); offset += 65536) {
        string bytes(31, '\0');
        files[i]->Read(&bytes[0], bytes.size(), offset);
        Check(bytes == expected.substr(offset, bytes.size()),
              "concurrent sparse range");
      }
    }));
  }
  for (auto &read : reads) {
    read.get();
  }
}

static void Mutate(const string &operation) {
  std::cout << "MUTATE " << operation << std::endl;
  string reply;
  std::getline(std::cin, reply);
  Check(reply == "OK", "fixture mutation failed");
}

static void CheckReadFailure(Connection &connection, const string &path) {
  auto file = FileSystem::GetFileSystem(*connection.context)
                  .OpenFile(path, FileFlags::FILE_FLAGS_READ);
  char bytes[16];
  Check(file->Read(bytes, sizeof(bytes)) == sizeof(bytes), "prime read window");
  Mutate("truncate_payload");
  // A miss that fails must invalidate the previously valid window and retain
  // the sequential cursor. Restoring the object permits a subsequent retry.
  Throws([&] { file->Read(bytes, sizeof(bytes), 1024 * 1024 + 100); },
         "truncated backing object");
  Check(file->SeekPosition() == sizeof(bytes), "I/O failure preserves cursor");
  Throws([&] { file->Read(bytes, sizeof(bytes)); },
         "failed refill invalidates old window");
  Check(file->SeekPosition() == sizeof(bytes),
        "sequential failure preserves cursor");
  Mutate("restore_payload");
  Check(file->Read(bytes, sizeof(bytes)) == sizeof(bytes),
        "retry after restoration");
}

static string Environment(const char *name, const char *fallback) {
  auto value = std::getenv(name);
  return value ? value : fallback;
}

static void S3Secret(Connection &connection, const string &name,
                     const string &scope, const string &endpoint,
                     const string &key, const string &secret) {
  Query(connection,
        "CREATE OR REPLACE SECRET " + name + " (TYPE LANCE, SCOPE " +
            Quote(scope) + ", ACCESS_KEY_ID " + Quote(key) +
            ", SECRET_ACCESS_KEY " + Quote(secret) +
            ", REGION 'us-east-1', ENDPOINT " + Quote(endpoint) +
            ", VIRTUAL_HOSTED_STYLE_REQUEST false, ALLOW_HTTP true)");
}

static void CheckRemoteSecretTransactions(const string &dataset,
                                          const string &endpoint) {
  auto root = dataset.substr(0, dataset.rfind('/'));
  auto key = Environment("LANCE_BLOB_S3_DATASET_KEY", "minioadmin");
  auto secret = Environment("LANCE_BLOB_S3_DATASET_SECRET", "minioadmin");
  auto external_key = Environment("LANCE_BLOB_S3_EXTERNAL_KEY", "minioadmin");
  auto external_secret =
      Environment("LANCE_BLOB_S3_EXTERNAL_SECRET", "minioadmin");
  auto expected = Expected();
  for (bool commit : {false, true}) {
    for (const auto &change :
         {string("create"), string("replace"), string("drop")}) {
      DuckDB database(nullptr);
      Load(database);
      auto connection = make_uniq<Connection>(database);
      auto &filesystem = FileSystem::GetFileSystem(*connection->context);
      const bool dropping = change == "drop";
      S3Secret(*connection, "dataset_fallback", root + "/", endpoint,
               dropping ? key : "invalid-key",
               dropping ? secret : "invalid-secret");
      S3Secret(*connection, "external_fallback", root + "/external-", endpoint,
               dropping ? external_key : "invalid-key",
               dropping ? external_secret : "invalid-secret");
      if (change != "create") {
        S3Secret(*connection, "dataset_scoped", dataset, endpoint,
                 "invalid-key", "invalid-secret");
        for (const auto &name : {string("a"), string("b")}) {
          S3Secret(*connection, "external_" + name,
                   root + "/external-" + name + ".bin", endpoint, "invalid-key",
                   "invalid-secret");
        }
      }
      connection->BeginTransaction();
      if (dropping) {
        Query(*connection, "DROP SECRET dataset_scoped");
        Query(*connection, "DROP SECRET external_a");
        Query(*connection, "DROP SECRET external_b");
      } else {
        S3Secret(*connection, "dataset_scoped", dataset, endpoint, key, secret);
        for (const auto &name : {string("a"), string("b")}) {
          S3Secret(*connection, "external_" + name,
                   root + "/external-" + name + ".bin", endpoint, external_key,
                   external_secret);
        }
      }
      auto paths = Routes(*connection, dataset);
      for (idx_t row : {idx_t(0), idx_t(1), idx_t(3), idx_t(4), idx_t(5)}) {
        auto result = Query(*connection, "SELECT content FROM read_blob(" +
                                             Quote(paths[row]) + ")");
        auto value = result->GetValue(0, 0);
        Check(StringValue::Get(value) == expected[row],
              "Blob read sees uncommitted secret " + change);
      }
      Connection observer(database);
      Throws(
          [&] {
            FileSystem::GetFileSystem(*observer.context)
                .OpenFile(paths[0], FileFlags::FILE_FLAGS_READ);
          },
          "uncommitted credentials are isolated from other connections");
      vector<unique_ptr<FileHandle>> retained;
      for (idx_t row : {idx_t(1), idx_t(3), idx_t(4)}) {
        retained.push_back(
            filesystem.OpenFile(paths[row], FileFlags::FILE_FLAGS_READ));
      }
      if (commit) {
        connection->Commit();
        CheckFile(observer, paths[0], expected[0]);
        if (!dropping) {
          connection->BeginTransaction();
          Query(*connection, "DROP SECRET dataset_scoped");
          Throws(
              [&] {
                filesystem.OpenFile(paths[0], FileFlags::FILE_FLAGS_READ);
              },
              "uncommitted drop cannot use a committed secret");
          connection->Rollback();
        }
      } else {
        connection->Rollback();
        Throws(
            [&] { filesystem.OpenFile(paths[0], FileFlags::FILE_FLAGS_READ); },
            "reopen observes secret rollback after " + change);
      }
      connection.reset();
      auto worker = std::async(std::launch::async, [&] {
        idx_t index = 0;
        for (idx_t row : {idx_t(1), idx_t(3), idx_t(4)}) {
          string bytes(16, '\0');
          retained[index++]->Read(&bytes[0], bytes.size(), 0);
          Check(bytes == expected[row].substr(0, bytes.size()),
                "open handle retains credentials after transaction end and "
                "disconnect");
        }
      });
      worker.get();
    }
  }
}

static void CheckRemoteEncodedScope(const string &dataset,
                                    const string &endpoint) {
  DuckDB database(nullptr);
  Load(database);
  Connection connection(database);
  auto root = dataset.substr(0, dataset.rfind('/'));
  auto key = Environment("LANCE_BLOB_S3_DATASET_KEY", "minioadmin");
  auto secret = Environment("LANCE_BLOB_S3_DATASET_SECRET", "minioadmin");
  auto alias = root + "%2F" + dataset.substr(dataset.rfind('/') + 1);
  S3Secret(connection, "fallback", root, endpoint, "invalid-key",
           "invalid-secret");
  S3Secret(connection, "encoded", root + "%2F", endpoint, key, secret);
  auto paths = Routes(connection, alias);
  Check(paths == Routes(connection, dataset),
        "encoded paths share the snapshot URI");
  auto expected = Expected();
  for (idx_t row : {idx_t(0), idx_t(1), idx_t(3)}) {
    auto result = Query(connection, "SELECT content FROM read_blob(" +
                                        Quote(paths[row]) + ")");
    auto value = result->GetValue(0, 0);
    Check(StringValue::Get(value) == expected[row],
          "Blob provider and scan select the same encoded scope");
  }
}

#ifdef LANCE_VANE_DISTRIBUTED
static void SessionS3Options(DuckDB &database, Connection &connection,
                             const string &endpoint, const string &key,
                             const string &secret) {
  // The standalone host has no httpfs. Register the standard setting shapes
  // used by the Vane session replay adapter, without adding production SQL.
  auto &config = DBConfig::GetConfig(*database.instance);
  for (const auto &name :
       {"s3_endpoint", "s3_access_key_id", "s3_secret_access_key", "s3_region",
        "s3_url_style"}) {
    if (!config.HasExtensionOption(name)) {
      config.AddExtensionOption(name, "Blob test session option",
                                LogicalType::VARCHAR, Value(""));
    }
  }
  if (!config.HasExtensionOption("s3_use_ssl")) {
    config.AddExtensionOption("s3_use_ssl", "Blob test session option",
                              LogicalType::BOOLEAN, Value::BOOLEAN(true));
  }
  Query(connection, "SET s3_endpoint=" + Quote(endpoint));
  Query(connection, "SET s3_access_key_id=" + Quote(key));
  Query(connection, "SET s3_secret_access_key=" + Quote(secret));
  Query(connection, "SET s3_region='us-east-1'");
  Query(connection, "SET s3_url_style='path'");
  Query(connection, "SET s3_use_ssl=false");
}
#endif

static void CheckRemote(const string &dataset, const string &endpoint) {
  CheckRemoteSecretTransactions(dataset, endpoint);
  CheckRemoteEncodedScope(dataset, endpoint);
  DuckDB database(nullptr);
  Load(database);
  Connection connection(database);
  auto root = dataset.substr(0, dataset.rfind('/'));
  auto key = Environment("LANCE_BLOB_S3_DATASET_KEY", "minioadmin");
  auto secret = Environment("LANCE_BLOB_S3_DATASET_SECRET", "minioadmin");
  auto external_key = Environment("LANCE_BLOB_S3_EXTERNAL_KEY", "minioadmin");
  auto external_secret =
      Environment("LANCE_BLOB_S3_EXTERNAL_SECRET", "minioadmin");
  S3Secret(connection, "blob_dataset", dataset, endpoint, key, secret);
  S3Secret(connection, "blob_external", root + "/external-", endpoint,
           external_key, external_secret);
#ifdef LANCE_VANE_DISTRIBUTED
  SessionS3Options(database, connection, endpoint, key, "");
#endif
  auto paths = Routes(connection, dataset);
  auto expected = Expected();
#ifdef LANCE_VANE_DISTRIBUTED
  {
    DuckDB overlay_database(nullptr);
    Load(overlay_database);
    Connection overlay(overlay_database);
    SessionS3Options(overlay_database, overlay, endpoint, key, secret);
    auto &overlay_fs = FileSystem::GetFileSystem(*overlay.context);
    auto captured = overlay_fs.OpenFile(paths[3], FileFlags::FILE_FLAGS_READ);
    Query(overlay, "SET s3_access_key_id='invalid-new-session-key'");
    string prefix(64, '\0');
    auto read = std::async(std::launch::async, [&] {
      captured->Read(&prefix[0], prefix.size(), 0);
    });
    read.get();
    Check(prefix == expected[3].substr(0, prefix.size()),
          "Vane opener retains captured session options");
    Throws([&] { overlay_fs.OpenFile(paths[3], FileFlags::FILE_FLAGS_READ); },
           "Vane reopen uses current session options");
  }
#endif
  for (idx_t row = 0; row < expected.size(); row++) {
    CheckFile(connection, paths[row], expected[row]);
  }
  auto &filesystem = FileSystem::GetFileSystem(*connection.context);
  // A more specific secret in the same bucket must win. Errors must not expose
  // the wrong credential or endpoint, and reopening must resolve new secrets.
  S3Secret(connection, "blob_external_b", root + "/external-b.bin", endpoint,
           "blob-deliberately-invalid", "blob-private-value");
  bool denied = false;
  try {
    filesystem.OpenFile(paths[5], FileFlags::FILE_FLAGS_READ);
  } catch (const std::exception &error) {
    auto message = string(error.what());
    Check(message.find("blob-private-value") == string::npos &&
              message.find("blob-deliberately-invalid") == string::npos,
          "secret redaction");
    denied = true;
  }
  Check(denied, "external target selects the most specific secret");
  CheckFile(connection, paths[4], expected[4]);
  S3Secret(connection, "blob_external_b", root + "/external-b.bin", endpoint,
           external_key, external_secret);
  Connection reopened(database);
  CheckFile(reopened, paths[5], expected[5]);

  // Count real HTTP requests outside metadata open. Compare the FileHandle's
  // bounded window against the same access bridge and Rust exact-range reader.
  auto file = filesystem.OpenFile(paths[3], FileFlags::FILE_FLAGS_READ);
  string bytes(65536, '\0');
  Mutate("buffered_begin");
  idx_t position = 0;
  while (position < expected[3].size()) {
    auto count = file->Read(&bytes[0], bytes.size());
    Check(count > 0 &&
              bytes.substr(0, count) == expected[3].substr(position, count),
          "remote buffered bytes");
    position += count;
  }
  Mutate("buffered_end");
  file->Close();
  LanceBlobAccess access(*connection.context);
  auto uri = ParseLanceBlobUri(paths[3]);
  LanceBlobOpenOptions options{uri.dataset.c_str(),  uri.version,
                               uri.snapshot.c_str(), uri.column.c_str(),
                               uri.row_id,           access.Handle()};
  void *raw = nullptr;
  uint64_t size = 0;
  Check(lance_blob_open(&options, &raw, &size) == 0,
        "unbuffered comparison open");
  try {
    Mutate("direct_begin");
    for (idx_t offset = 0; offset < size; offset += bytes.size()) {
      auto count = MinValue<idx_t>(bytes.size(), size - offset);
      size_t read = 0;
      Check(lance_blob_read_at(raw, offset,
                               reinterpret_cast<uint8_t *>(&bytes[0]), count,
                               &read) == 0,
            "unbuffered range");
      Check(read == count &&
                bytes.substr(0, count) == expected[3].substr(offset, count),
            "unbuffered bytes");
    }
    Mutate("direct_end");
  } catch (...) {
    lance_blob_free(raw);
    throw;
  }
  lance_blob_free(raw);

  auto head_reader = filesystem.OpenFile(paths[3], FileFlags::FILE_FLAGS_READ);
  auto tail_reader = filesystem.OpenFile(paths[3], FileFlags::FILE_FLAGS_READ);
  Mutate("concurrent_begin");
  auto read_edges = [&](FileHandle &reader) {
    string small(64, '\0');
    for (auto offset : vector<idx_t>{0, expected[3].size() - small.size()}) {
      reader.Read(&small[0], small.size(), offset);
      Check(small == expected[3].substr(offset, small.size()),
            "concurrent remote head/tail bytes");
    }
    Check(reader.SeekPosition() == 0, "independent remote positional cursor");
  };
  auto first =
      std::async(std::launch::async, [&] { read_edges(*head_reader); });
  auto second =
      std::async(std::launch::async, [&] { read_edges(*tail_reader); });
  first.get();
  second.get();
  Mutate("concurrent_end");
  head_reader->Close();
  tail_reader->Close();

  auto sparse = filesystem.OpenFile(paths[3], FileFlags::FILE_FLAGS_READ);
  Mutate("sparse_begin");
  string small(31, '\0');
  for (auto offset : vector<idx_t>{17, 524288, 1572864, 1024, 2097152}) {
    sparse->Seek(offset);
    Check(sparse->Read(&small[0], small.size()) == int64_t(small.size()) &&
              small == expected[3].substr(offset, small.size()),
          "remote sparse seek bytes");
  }
  Mutate("sparse_end");
  sparse->Close();
  // Policy rejections must result in zero requests through the proxy.
  Mutate("denied_begin");
  DuckDB restricted(nullptr);
  Load(restricted);
  Connection denied_connection(restricted);
  Query(denied_connection, "SET allowed_paths=[" + Quote(paths[3]) + "]");
  Query(denied_connection, "SET enable_external_access=false");
  Throws(
      [&] {
        FileSystem::GetFileSystem(*denied_connection.context)
            .FileExists(paths[3]);
      },
      "denied remote backing");
  Mutate("denied_end");
  Query(connection, "SET disabled_filesystems='S3FileSystem'");
  Throws([&] { filesystem.FileExists(paths[3]); },
         "disabled S3 cannot use Rust I/O");
}

static void CheckSnapshots(Connection &connection, const string &dataset,
                           const vector<string> &paths) {
  // Binding FROM captures the scan before binding the SELECT expression. The
  // test-only function appends exactly between that scan and Blob URI binding.
  ScalarFunction append(
      "blob_test_append", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
      [](DataChunk &input, ExpressionState &, Vector &result) {
        result.Reference(input.data[0]);
      },
      [](ClientContext &, ScalarFunction &,
         vector<unique_ptr<Expression>> &) -> unique_ptr<FunctionData> {
        Mutate("append");
        return nullptr;
      });
  ExtensionLoader loader(DatabaseInstance::GetDatabase(*connection.context),
                         "blob_test");
  loader.RegisterFunction(append);
  auto prepared = connection.Prepare(RouteSQL(dataset));
  Check(!prepared->HasError(), "prepare route query");
  auto alias =
      connection.Prepare(RouteSQL(ParseLanceBlobUri(paths[0]).dataset));
  Check(!alias->HasError(), "prepare file URI alias");
  auto initial = prepared->Execute();
  Check(!initial->HasError(), "execute initial prepared route query");
  auto initial_chunk = initial->Fetch();
  Check(initial_chunk && initial_chunk->size() == 8 &&
            initial_chunk->GetValue(0, 0).GetValue<string>() == paths[0],
        "prepared query initially captures version one");
  initial.reset();
  auto during =
      Query(connection, "SELECT lance_blob_uri(blob_test_append(" +
                            Quote(dataset) + "), 'video', _rowid) FROM " +
                            Quote(dataset) + " ORDER BY id");
  Check(during->RowCount() == 8, "scan retained before concurrent append");
  for (idx_t row = 0; row < during->RowCount(); row++) {
    Check(during->GetValue(0, row).GetValue<string>() == paths[row],
          "scan and URI bind share pre-append snapshot");
  }
  connection.BeginTransaction();
  LanceInvalidateDatasetCacheForPath(*connection.context, dataset);
  connection.Commit();
  // DuckDB requires rebinding CONSISTENT_WITHIN_QUERY functions at execution.
  // The scan and URI must both advance, while materialized old URIs stay valid.
  auto rebound = prepared->Execute();
  Check(!rebound->HasError(), "execute rebound prepared route query");
  auto chunk = rebound->Fetch();
  Check(chunk && chunk->size() == 9, "prepared scan rebinds to version two");
  auto fresh = Routes(connection, dataset);
  for (idx_t row = 0; row < chunk->size(); row++) {
    Check(chunk->GetValue(0, row).GetValue<string>() == fresh[row],
          "prepared URI and scan rebind to identical snapshot");
  }
  auto alias_result = alias->Execute();
  Check(!alias_result->HasError(), "execute file URI alias");
  auto alias_chunk = alias_result->Fetch();
  Check(alias_chunk && alias_chunk->size() == 9 &&
            alias_chunk->GetValue(0, 0).GetValue<string>() == fresh[0],
        "file URI alias shares rebound snapshot");
  Check(fresh.size() == 9 && ParseLanceBlobUri(fresh[8]).version == 2,
        "rebound scan and URI share new version");
  CheckFile(connection, paths[0], "abc");
  Mutate("compact");
  CheckFile(connection, paths[0], "abc");
  Mutate("replace");
  Throws(
      [&] {
        FileSystem::GetFileSystem(*connection.context).FileExists(paths[0]);
      },
      "same-path replacement rejected");
  Mutate("remove_versions");
  Throws(
      [&] {
        FileSystem::GetFileSystem(*connection.context).FileExists(paths[0]);
      },
      "missing historical version rejected");
}

static void CheckHuggingFace(const string &dataset, const string &revision) {
  DuckDB database(nullptr);
  Load(database);
  Connection connection(database);
  Query(connection, "CREATE SECRET blob_hf (TYPE LANCE, SCOPE "
                    "'hf://datasets/lance-format/pusht-lance/', "
                    "HF_REVISION " +
                        Quote(revision) + ")");
  auto result =
      Query(connection, "SELECT lance_blob_uri(" + Quote(dataset) +
                            ", 'video_bytes', _rowid) FROM " + Quote(dataset) +
                            " ORDER BY chunk_index, file_index LIMIT 1");
  auto path = result->GetValue(0, 0).GetValue<string>();
  auto file = FileSystem::GetFileSystem(*connection.context)
                  .OpenFile(path, FileFlags::FILE_FLAGS_READ);
  Check(file->GetFileSize() == 6890970, "pinned HF payload size");
  char bytes[64];
  Check(file->Read(bytes, sizeof(bytes)) == sizeof(bytes),
        "HF sequential prefix");
  Check(string(bytes, 12) == string("\0\0\0\x20"
                                    "ftypisom",
                                    12),
        "HF MP4 prefix");
  file->Read(bytes, sizeof(bytes), 6735984);
  Check(string(bytes + 4, 4) == "moov" && file->SeekPosition() == 64,
        "HF positional MP4 metadata");
  file->Seek(0);
  Check(file->Read(bytes, 12) == 12 && string(bytes + 4, 4) == "ftyp",
        "HF backward seek");
  file->Close();
  Query(connection, "SET disabled_filesystems='HuggingFaceFileSystem'");
  Throws(
      [&] { FileSystem::GetFileSystem(*connection.context).FileExists(path); },
      "disabled HF backing");
}

static void Run(const string &dataset, const string &external,
                const string &snapshot_dataset) {
  CheckAccessRegressions(dataset);
  DuckDB database(nullptr);
  Load(database);
  Connection connection(database);
  auto paths = Routes(connection, dataset);
  Check(paths.size() == 8, "fixture rows");
  CheckSql(connection, dataset);
  CheckUriParser(paths[0]);
  auto expected = Expected();
  for (idx_t row = 0; row < expected.size(); row++) {
    CheckFile(connection, paths[row], expected[row]);
  }
  auto nested = Routes(connection, dataset, "info.`clip.with.dot`");
  CheckFile(connection, nested[0], "abc");
  CheckFlagsAndErrors(connection, paths);
  CheckPolicy(dataset, external, paths);
  CheckLifetimeAndConcurrency(paths[3], expected[3]);
  if (!snapshot_dataset.empty()) {
    CheckReadFailure(connection, paths[3]);
    CheckSnapshots(connection, snapshot_dataset,
                   Routes(connection, snapshot_dataset));
  }
}

} // namespace duckdb

int main(int argc, char **argv) {
  try {
    if (argc < 3) {
      throw std::runtime_error(
          "Usage: lance_blob_filesystem_test DATASET EXTERNAL [--mutations]");
    }
    if (std::string(argv[1]) == "--access-regressions") {
      duckdb::CheckAccessRegressions(argv[2]);
    } else if (argc > 3 && std::string(argv[1]) == "--remote") {
      duckdb::CheckRemote(argv[2], argv[3]);
    } else if (argc > 3 && std::string(argv[1]) == "--hf") {
      duckdb::CheckHuggingFace(argv[2], argv[3]);
    } else {
      duckdb::Run(argv[1], argv[2], argc > 3 ? argv[3] : "");
    }
    std::cout << "Blob filesystem tests passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
