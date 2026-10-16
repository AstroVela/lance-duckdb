#include "lance_blob.hpp"

#include "lance_blob_access.hpp"
#include "lance_common.hpp"
#include "lance_dataset_cache.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "utf8proc_wrapper.hpp"

#include <limits>

namespace duckdb {

static constexpr idx_t MAX_BLOB_URI = 64 * 1024;
static constexpr idx_t MAX_BLOB_DATASET = 16 * 1024;
static constexpr idx_t MAX_BLOB_COLUMN = 4 * 1024;
static constexpr const char *BLOB_URI_PREFIX = "lance-blob://v1/?";

static void InvalidUri() {
  throw InvalidInputException("Invalid lance-blob URI");
}

static bool Unreserved(unsigned char byte) {
  return (byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') ||
         (byte >= '0' && byte <= '9') || byte == '-' || byte == '.' ||
         byte == '_' || byte == '~';
}

static string EncodeValue(const string &value) {
  static constexpr char HEX[] = "0123456789ABCDEF";
  string result;
  for (unsigned char byte : value) {
    if (Unreserved(byte)) {
      result += char(byte);
    } else {
      result += '%';
      result += HEX[byte >> 4];
      result += HEX[byte & 15];
    }
  }
  return result;
}

static uint8_t HexDigit(char c) {
  if (c >= '0' && c <= '9') {
    return uint8_t(c - '0');
  }
  if (c >= 'a' && c <= 'f') {
    return uint8_t(c - 'a' + 10);
  }
  if (c >= 'A' && c <= 'F') {
    return uint8_t(c - 'A' + 10);
  }
  InvalidUri();
  return 0;
}

static string DecodeValue(const string &value) {
  string result;
  for (idx_t index = 0; index < value.size(); index++) {
    auto byte = value[index];
    if (byte == '%') {
      if (value.size() - index < 3) {
        InvalidUri();
      }
      byte =
          char((HexDigit(value[index + 1]) << 4) | HexDigit(value[index + 2]));
      index += 2;
    } else if (!Unreserved(static_cast<unsigned char>(byte))) {
      InvalidUri();
    }
    if (!byte) {
      InvalidUri();
    }
    result += byte;
  }
  if (!Utf8Proc::IsValid(result.data(), result.size())) {
    InvalidUri();
  }
  return result;
}

static uint64_t ParseUnsigned(const string &value) {
  if (value.empty()) {
    InvalidUri();
  }
  uint64_t result = 0;
  for (auto byte : value) {
    if (byte < '0' || byte > '9' ||
        result > (std::numeric_limits<uint64_t>::max() - uint64_t(byte - '0')) /
                     10) {
      InvalidUri();
    }
    result = result * 10 + uint64_t(byte - '0');
  }
  return result;
}

static void ValidateUri(const LanceBlobUri &uri) {
  if (uri.dataset.empty() || uri.dataset.size() > MAX_BLOB_DATASET ||
      uri.column.empty() || uri.column.size() > MAX_BLOB_COLUMN ||
      uri.version == 0 || uri.snapshot.size() != 64 ||
      uri.dataset.find('\0') != string::npos ||
      uri.column.find('\0') != string::npos ||
      !Utf8Proc::IsValid(uri.dataset.data(), uri.dataset.size()) ||
      !Utf8Proc::IsValid(uri.column.data(), uri.column.size())) {
    InvalidUri();
  }
  for (auto c : uri.snapshot) {
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
      InvalidUri();
    }
  }
}

static string EncodePrefix(const LanceBlobUri &uri) {
  ValidateUri(uri);
  auto prefix =
      string(BLOB_URI_PREFIX) + "dataset=" + EncodeValue(uri.dataset) +
      "&version=" + to_string(uri.version) + "&snapshot=" + uri.snapshot +
      "&column=" + EncodeValue(uri.column) + "&rowid=";
  if (prefix.size() + 20 > MAX_BLOB_URI) {
    InvalidUri();
  }
  return prefix;
}

string EncodeLanceBlobUri(const LanceBlobUri &uri) {
  return EncodePrefix(uri) + to_string(uri.row_id);
}

LanceBlobUri ParseLanceBlobUri(const string &path) {
  if (path.size() > MAX_BLOB_URI ||
      !StringUtil::StartsWith(path, BLOB_URI_PREFIX)) {
    InvalidUri();
  }
  auto query = path.substr(string(BLOB_URI_PREFIX).size());
  if (query.empty() || query.front() == '&' || query.back() == '&' ||
      query.find("&&") != string::npos) {
    InvalidUri();
  }
  unordered_map<string, string> fields;
  for (const auto &item : StringUtil::Split(query, '&')) {
    auto equal = item.find('=');
    if (equal == string::npos ||
        !fields.emplace(item.substr(0, equal), item.substr(equal + 1)).second) {
      InvalidUri();
    }
  }
  if (fields.size() != 5 || !fields.count("dataset") ||
      !fields.count("version") || !fields.count("snapshot") ||
      !fields.count("column") || !fields.count("rowid")) {
    InvalidUri();
  }
  LanceBlobUri uri;
  uri.dataset = DecodeValue(fields.at("dataset"));
  uri.column = DecodeValue(fields.at("column"));
  uri.snapshot = fields.at("snapshot");
  uri.version = ParseUnsigned(fields.at("version"));
  uri.row_id = ParseUnsigned(fields.at("rowid"));
  ValidateUri(uri);
  // All generated locators have a scheme and an absolute local path. Do not
  // reinterpret a hand-edited relative locator against the opener's cwd.
  if (uri.dataset.find("://") == string::npos) {
    InvalidUri();
  }
  uri.dataset = LanceNormalizeBlobTarget(uri.dataset);
  return uri;
}

struct BlobUriBindData final : FunctionData {
  string prefix;
  shared_ptr<LanceDatasetCacheEntry> entry;

  unique_ptr<FunctionData> Copy() const override {
    auto copy = make_uniq<BlobUriBindData>();
    copy->prefix = prefix;
    copy->entry = entry;
    return std::move(copy);
  }
  bool Equals(const FunctionData &other) const override {
    return prefix == other.Cast<BlobUriBindData>().prefix;
  }
};

static unique_ptr<FunctionData>
BindBlobUri(ClientContext &context, ScalarFunction &,
            vector<unique_ptr<Expression>> &arguments) {
  vector<string> constants;
  for (idx_t index = 0; index < 2; index++) {
    if (arguments[index]->HasParameter()) {
      throw ParameterNotResolvedException();
    }
    if (!arguments[index]->IsFoldable()) {
      throw BinderException(
          "lance_blob_uri dataset and column must be non-null constants");
    }
    auto value = ExpressionExecutor::EvaluateScalar(context, *arguments[index]);
    if (value.IsNull()) {
      throw BinderException(
          "lance_blob_uri dataset and column must be non-null constants");
    }
    constants.push_back(value.GetValue<string>());
  }
  LanceBlobUri uri;
  if (constants[0].empty() || constants[0].size() > MAX_BLOB_DATASET ||
      constants[1].empty() || constants[1].size() > MAX_BLOB_COLUMN ||
      constants[0].find('\0') != string::npos ||
      constants[1].find('\0') != string::npos) {
    throw BinderException(
        "lance_blob_uri requires valid dataset and column strings");
  }
  uri.dataset = LanceNormalizeBlobTarget(LanceNormalizeS3Scheme(constants[0]));
  uri.column = constants[1];
  auto &database = DatabaseInstance::GetDatabase(context);
  LanceCheckBlobAccess(database, uri.dataset, LANCE_BLOB_PROVIDER);
  if (FileSystem::GetFileSystem(database).SubSystemIsDisabled(
          "LanceBlobFileSystem")) {
    throw PermissionException("Lance Blob filesystem is disabled");
  }
  LanceBlobAccess access(context);
  auto data = make_uniq<BlobUriBindData>();
  data->entry = LanceGetOrOpenDatasetEntry(context, uri.dataset, nullptr,
                                           access.Handle());
  if (!data->entry) {
    LanceThrowBlobError("bind");
  }
#ifdef LANCE_VANE_DISTRIBUTED
  // Binding can fail during column or snapshot validation. Register expiry
  // as soon as the entry exists so QueryEnd also clears failed binds.
  LanceExpireDatasetCacheAfterQuery(context, uri.dataset);
#endif
  if (lance_blob_validate_column(data->entry->Handle(), uri.column.c_str()) !=
      0) {
    LanceConsumeLastError();
    throw BinderException(
        "lance_blob_uri requires a Blob v2 column reached through structs");
  }
  uri.version = lance_dataset_version(data->entry->Handle());
  auto token =
      lance_blob_snapshot_token(data->entry->Handle(), access.Handle());
  if (!token) {
    LanceThrowBlobError("capture snapshot");
  }
  uri.snapshot = token;
  lance_free_string(token);
  data->prefix = EncodePrefix(uri);
  return std::move(data);
}

static void BlobUriFunction(DataChunk &input, ExpressionState &state,
                            Vector &result) {
  auto &expression = state.expr.Cast<BoundFunctionExpression>();
  auto &data = expression.bind_info->Cast<BlobUriBindData>();
  UnifiedVectorFormat rows;
  input.data[2].ToUnifiedFormat(input.size(), rows);
  auto row_ids = UnifiedVectorFormat::GetData<uint64_t>(rows);
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto output = FlatVector::GetData<string_t>(result);
  auto &validity = FlatVector::Validity(result);
  for (idx_t index = 0; index < input.size(); index++) {
    auto source = rows.sel->get_index(index);
    if (!rows.validity.RowIsValid(source)) {
      validity.SetInvalid(index);
    } else {
      output[index] = StringVector::AddString(
          result, data.prefix + to_string(row_ids[source]));
    }
  }
}

void RegisterLanceBlobFunctions(ExtensionLoader &loader) {
  ScalarFunction function(
      "lance_blob_uri",
      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT},
      LogicalType::VARCHAR, BlobUriFunction, BindBlobUri);
  function.stability = FunctionStability::CONSISTENT_WITHIN_QUERY;
  function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
  function.SetSerializeCallback([](Serializer &,
                                   const optional_ptr<FunctionData>,
                                   const ScalarFunction &) {
    throw NotImplementedException(
        "lance_blob_uri bind state cannot be serialized; materialize the URI "
        "string locally");
  });
  function.SetDeserializeCallback(
      [](Deserializer &, ScalarFunction &) -> unique_ptr<FunctionData> {
        throw NotImplementedException(
            "lance_blob_uri bind state cannot be deserialized");
      });
  loader.RegisterFunction(function);
}

} // namespace duckdb
