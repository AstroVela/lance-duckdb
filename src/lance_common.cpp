#include "lance_common.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#ifdef LANCE_VANE_DISTRIBUTED
#include "duckdb/common/file_system.hpp"
#endif
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "lance_ffi.hpp"
#include "lance_session_state.hpp"
#include "lance_table_entry.hpp"
#ifdef LANCE_VANE_DISTRIBUTED
#include <algorithm>
#include <cstdlib>
#endif
#include <cstring>

namespace duckdb {

string LanceConsumeLastError() {
  auto code = lance_last_error_code();
  string message;
  if (auto *ptr = lance_last_error_message()) {
    message = ptr;
    lance_free_string(ptr);
  }

  if (code == 0 && message.empty()) {
    return "";
  }
  if (message.empty()) {
    return "code=" + to_string(code);
  }
  if (code == 0) {
    return message;
  }
  return message + " (code=" + to_string(code) + ")";
}

string LanceFormatErrorSuffix() {
  auto err = LanceConsumeLastError();
  if (err.empty()) {
    return "";
  }
  return " (Lance error: " + err + ")";
}

bool IsComputedSearchColumn(const string &name) {
  return name == "_distance" || name == "_score" || name == "_hybrid_score";
}

static void BuildStringPointerArray(const vector<string> &values,
                                    vector<const char *> &out_ptrs) {
  out_ptrs.clear();
  out_ptrs.reserve(values.size());
  for (auto &value : values) {
    out_ptrs.push_back(value.c_str());
  }
}

void FillLanceNamespaceQueryConfig(
    ClientContext &context, const LanceNamespaceTableConfig &cfg, uint64_t k,
    bool prefilter, const string &filter, const vector<string> &columns,
    vector<const char *> &option_key_ptrs,
    vector<const char *> &option_value_ptrs, vector<const char *> &column_ptrs,
    string &bearer_token, string &api_key,
    LanceNamespaceQueryConfig &out_config) {
  static constexpr uint8_t NAMESPACE_KIND_DIRECTORY = 0;
  static constexpr uint8_t NAMESPACE_KIND_REST = 1;

  out_config = {};
  out_config.table_id = cfg.table_id.c_str();
  out_config.k = k;
#ifdef LANCE_VANE_DISTRIBUTED
  out_config.version = NumericCast<int64_t>(cfg.snapshot_version);
#endif
  out_config.prefilter = prefilter ? 1 : 0;
  out_config.filter = filter.empty() ? nullptr : filter.c_str();

  BuildStringPointerArray(columns, column_ptrs);
  out_config.columns = column_ptrs.empty() ? nullptr : column_ptrs.data();
  out_config.columns_len = column_ptrs.size();

  if (cfg.IsDirectory()) {
    out_config.namespace_kind = NAMESPACE_KIND_DIRECTORY;
    out_config.root = cfg.root.c_str();
    BuildStorageOptionPointerArrays(cfg.option_keys, cfg.option_values,
                                    option_key_ptrs, option_value_ptrs);
    out_config.option_keys =
        option_key_ptrs.empty() ? nullptr : option_key_ptrs.data();
    out_config.option_values =
        option_value_ptrs.empty() ? nullptr : option_value_ptrs.data();
    out_config.options_len = option_key_ptrs.size();
    return;
  }

  out_config.namespace_kind = NAMESPACE_KIND_REST;
  out_config.endpoint = cfg.endpoint.c_str();
  out_config.delimiter =
      cfg.delimiter.empty() ? nullptr : cfg.delimiter.c_str();
  out_config.headers_tsv =
      cfg.headers_tsv.empty() ? nullptr : cfg.headers_tsv.c_str();

  unordered_map<string, Value> overrides;
  if (!cfg.bearer_token_override.empty()) {
    overrides["bearer_token"] = Value(cfg.bearer_token_override);
  }
  if (!cfg.api_key_override.empty()) {
    overrides["api_key"] = Value(cfg.api_key_override);
  }
  ResolveLanceNamespaceAuth(context, cfg.endpoint, overrides, bearer_token,
                            api_key);
  out_config.bearer_token =
      bearer_token.empty() ? nullptr : bearer_token.c_str();
  out_config.api_key = api_key.empty() ? nullptr : api_key.c_str();
}

#ifdef LANCE_VANE_DISTRIBUTED
static bool IsLanceVaneUriSchemeFirstChar(char c) {
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

static bool IsLanceVaneUriSchemeChar(char c) {
  return IsLanceVaneUriSchemeFirstChar(c) || (c >= '0' && c <= '9') ||
         c == '+' || c == '-' || c == '.';
}

string LanceVaneCanonicalizeSecretScope(const string &scope) {
  auto delimiter = scope.find("://");
  if (delimiter == string::npos || delimiter == 0 ||
      !IsLanceVaneUriSchemeFirstChar(scope[0])) {
    return scope;
  }
  for (idx_t i = 1; i < delimiter; i++) {
    if (!IsLanceVaneUriSchemeChar(scope[i])) {
      return scope;
    }
  }

  string scheme;
  scheme.reserve(delimiter);
  for (idx_t i = 0; i < delimiter; i++) {
    auto c = scope[i];
    if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c + ('a' - 'A'));
    }
    scheme.push_back(c);
  }
  if (scheme == "s3a" || scheme == "s3n") {
    scheme = "s3";
  }
  return scheme + scope.substr(delimiter);
}
#endif

string LanceNormalizeS3Scheme(const string &path) {
#ifdef LANCE_VANE_DISTRIBUTED
  // URL schemes are ASCII case-insensitive. Canonicalize all supported S3
  // spellings before secret matching and connection-setting replay so a
  // coordinator cannot succeed from ambient credentials while an isolated
  // worker misses the captured DuckDB s3_* state.
  if (path.size() >= 5 && StringUtil::CIEquals(path.substr(0, 5), "s3://")) {
    return "s3://" + path.substr(5);
  }
  if (path.size() >= 6 && (StringUtil::CIEquals(path.substr(0, 6), "s3a://") ||
                           StringUtil::CIEquals(path.substr(0, 6), "s3n://"))) {
    return "s3://" + path.substr(6);
  }
#else
  if (StringUtil::StartsWith(path, "s3a://")) {
    return "s3://" + path.substr(6);
  }
  if (StringUtil::StartsWith(path, "s3n://")) {
    return "s3://" + path.substr(6);
  }
#endif
  return path;
}

#ifdef LANCE_VANE_DISTRIBUTED
static uint8_t LanceVaneClassifyPath(const string &path) {
  return lance_vane_classify_path(
      reinterpret_cast<const uint8_t *>(path.data()), path.size());
}

bool LanceVanePathHasPrivateUriComponents(const string &path) {
  auto classification = LanceVaneClassifyPath(path);
  return classification &
         (LANCE_VANE_PATH_HAS_PRIVATE_COMPONENTS | LANCE_VANE_PATH_INVALID);
}

bool LanceVanePathIsRemote(const string &path) {
  return LanceVaneClassifyPath(path) & LANCE_VANE_PATH_IS_REMOTE;
}

bool LanceVanePathRequiresRedaction(ClientContext &context,
                                    const string &path) {
  auto classification = LanceVaneClassifyPath(path);
  if (classification & (LANCE_VANE_PATH_HAS_PRIVATE_COMPONENTS |
                        LANCE_VANE_PATH_INVALID | LANCE_VANE_PATH_IS_REMOTE)) {
    return true;
  }
  string open_path;
  vector<string> option_keys;
  vector<string> option_values;
  ResolveLanceStorageOptionsForDistributedRead(context, path, open_path,
                                               option_keys, option_values);
  // Backend errors can repeat any resolved option, including temporary
  // credentials or a private endpoint. The values themselves are never
  // retained here; option presence is sufficient to fail closed.
  return !option_keys.empty();
}

bool LanceVanePathIsLanceDataset(const string &path) {
  return LanceVaneClassifyPath(path) & LANCE_VANE_PATH_IS_LANCE_DATASET;
}

string LanceVaneDiagnosticPath(const string &path, bool force_redaction) {
  if (force_redaction || LanceVanePathHasPrivateUriComponents(path) ||
      LanceVanePathIsRemote(path)) {
    return "<redacted-private-uri>";
  }
  return path;
}

string LanceVaneFormatErrorSuffix(const string &path, bool force_redaction) {
  if (!force_redaction && !LanceVanePathHasPrivateUriComponents(path) &&
      !LanceVanePathIsRemote(path)) {
    return LanceFormatErrorSuffix();
  }
  auto error = LanceConsumeLastError();
  return error.empty() ? "" : " (Lance error details redacted)";
}

string LanceVaneReplayPath(ClientContext &context, const string &path) {
  if (path.find('\0') != string::npos) {
    return "";
  }
  auto classification = LanceVaneClassifyPath(path);
  if (classification &
      (LANCE_VANE_PATH_HAS_PRIVATE_COMPONENTS |
       LANCE_VANE_PATH_IS_PROCESS_LOCAL | LANCE_VANE_PATH_INVALID)) {
    return "";
  }
  if (!(classification & LANCE_VANE_PATH_IS_URI)) {
    auto &file_system = FileSystem::GetFileSystem(context);
    auto expanded = file_system.ExpandPath(path);
    if (file_system.IsPathAbsolute(expanded)) {
      return expanded;
    }
    return file_system.JoinPath(FileSystem::GetWorkingDirectory(), expanded);
  }
  return path;
}
#endif

string LanceDirectoryNamespaceDatasetUri(const LanceNamespaceTableConfig &cfg) {
  if (!cfg.display_uri.empty()) {
    return LanceNormalizeS3Scheme(cfg.display_uri);
  }

  auto child = cfg.table_id;
  if (!StringUtil::EndsWith(child, ".lance")) {
    child += ".lance";
  }
  string uri;
  if (cfg.root.empty()) {
    uri = std::move(child);
  } else if (cfg.root.back() == '/' || cfg.root.back() == '\\') {
    uri = cfg.root + child;
  } else {
    uri = cfg.root + "/" + child;
  }
  return LanceNormalizeS3Scheme(uri);
}

#ifdef LANCE_VANE_DISTRIBUTED
bool LanceVaneTablePathRequiresRedaction(const LanceTableEntry &table,
                                         const string &path) {
  if (LanceVanePathHasPrivateUriComponents(path) ||
      LanceVanePathHasPrivateUriComponents(table.DatasetUri()) ||
      LanceVanePathIsRemote(path) ||
      LanceVanePathIsRemote(table.DatasetUri())) {
    return true;
  }
  if (!table.IsNamespaceBacked()) {
    return false;
  }

  auto &cfg = table.NamespaceConfig();
  if (cfg.IsDirectory()) {
    return cfg.distributed_replay_path_restricted ||
           cfg.uses_coordinator_storage_secret || !cfg.option_keys.empty() ||
           LanceVanePathHasPrivateUriComponents(cfg.root) ||
           LanceVanePathHasPrivateUriComponents(cfg.display_uri) ||
           LanceVanePathIsRemote(cfg.root) ||
           LanceVanePathIsRemote(cfg.display_uri);
  }
  // REST namespace implementations can return opaque, short-lived storage
  // options internally. Even an otherwise public endpoint can therefore make
  // a backend error repeat credentials that C++ never observes directly.
  return true;
}

string LanceVaneTableDiagnosticPath(const LanceTableEntry &table,
                                    const string &path) {
  return LanceVaneDiagnosticPath(
      path, LanceVaneTablePathRequiresRedaction(table, path));
}

string LanceVaneTableFormatErrorSuffix(const LanceTableEntry &table,
                                       const string &path) {
  return LanceVaneFormatErrorSuffix(
      path, LanceVaneTablePathRequiresRedaction(table, path));
}
#endif

static string SecretValueToString(const Value &value) {
  if (value.IsNull()) {
    return "";
  }
  return value.ToString();
}

void LanceFillStorageOptionsFromSecrets(ClientContext &context,
                                        const string &path,
                                        vector<string> &out_keys,
                                        vector<string> &out_values) {
  auto &secret_manager = SecretManager::Get(context);
  auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
#ifdef LANCE_VANE_DISTRIBUTED
  auto lookup_path = LanceVaneCanonicalizeSecretScope(path);
  auto secret_match =
      secret_manager.LookupSecret(transaction, lookup_path, "lance");
#else
  auto secret_match = secret_manager.LookupSecret(transaction, path, "lance");
#endif
  if (!secret_match.HasMatch() || !secret_match.secret_entry ||
      !secret_match.secret_entry->secret) {
    return;
  }

  auto *kv_secret = dynamic_cast<const KeyValueSecret *>(
      secret_match.secret_entry->secret.get());
  if (!kv_secret) {
    return;
  }

  for (auto &kv : kv_secret->secret_map) {
    if (kv.second.IsNull()) {
      continue;
    }
    out_keys.push_back(kv.first);
    out_values.push_back(kv.second.ToString());
  }
}

#ifdef LANCE_VANE_DISTRIBUTED
static bool ProcessHasLanceS3Environment() {
  static constexpr const char *AWS_ENVIRONMENT_KEYS[] = {
      "AWS_ACCESS_KEY_ID",
      "AWS_SECRET_ACCESS_KEY",
      "AWS_SESSION_TOKEN",
      "AWS_REGION",
      "AWS_DEFAULT_REGION",
      "AWS_ENDPOINT_URL",
      "AWS_PROFILE",
      "AWS_SHARED_CREDENTIALS_FILE",
      "AWS_CONFIG_FILE",
      "AWS_WEB_IDENTITY_TOKEN_FILE",
      "AWS_ROLE_ARN",
      "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
      "AWS_CONTAINER_CREDENTIALS_FULL_URI",
  };
  for (auto *key : AWS_ENVIRONMENT_KEYS) {
    auto *value = std::getenv(key);
    if (value && value[0]) {
      return true;
    }
  }
  return false;
}

#ifdef LANCE_VANE_DISTRIBUTED
static bool TryGetLanceS3Setting(ClientContext &context, const string &name,
                                 string &out_value,
                                 SettingScope *out_scope = nullptr,
                                 bool accept_empty_local = false) {
#else
static bool TryGetLanceS3Setting(ClientContext &context, const string &name,
                                 string &out_value,
                                 SettingScope *out_scope = nullptr) {
#endif
  Value value;
  auto lookup_result = context.TryGetCurrentSetting(name, value);
  if (!lookup_result || value.IsNull()) {
    return false;
  }
  out_value = value.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
#ifdef LANCE_VANE_DISTRIBUTED
  auto scope = lookup_result.GetScope();
  if (out_scope) {
    *out_scope = scope;
  }
  if (out_value.empty()) {
    return accept_empty_local && scope == SettingScope::LOCAL;
  }
#else
  if (out_value.empty()) {
    return false;
  }
  if (out_scope) {
    *out_scope = lookup_result.GetScope();
  }
#endif
  return true;
}

static string LanceS3EnvironmentValue(const char *name) {
  auto *value = std::getenv(name);
  return value ? string(value) : string();
}

static bool LanceS3SettingsOverrideProcessEnvironment(ClientContext &context) {
  string endpoint;
  if (TryGetLanceS3Setting(context, "s3_endpoint", endpoint)) {
    return true;
  }

  string access_key;
  string secret_key;
#ifdef LANCE_VANE_DISTRIBUTED
  auto access_key_scope = SettingScope::INVALID;
  auto secret_key_scope = SettingScope::INVALID;
  auto has_access_key = TryGetLanceS3Setting(context, "s3_access_key_id",
                                             access_key, &access_key_scope);
  auto has_secret_key = TryGetLanceS3Setting(context, "s3_secret_access_key",
                                             secret_key, &secret_key_scope);
  auto has_local_access_key = access_key_scope == SettingScope::LOCAL;
  auto has_local_secret_key = secret_key_scope == SettingScope::LOCAL;
  if (has_local_access_key != has_local_secret_key) {
    throw InvalidInputException(
        "Connection-local S3 credentials must set both s3_access_key_id and "
        "s3_secret_access_key");
  }
  if (has_local_access_key && (!has_access_key || !has_secret_key)) {
    throw InvalidInputException(
        "Connection-local S3 access and secret keys must be non-empty");
  }
  if (has_local_access_key) {
    return true;
  }
#else
  auto has_access_key =
      TryGetLanceS3Setting(context, "s3_access_key_id", access_key);
  auto has_secret_key =
      TryGetLanceS3Setting(context, "s3_secret_access_key", secret_key);
#endif
  if (has_access_key && has_secret_key &&
      (access_key != LanceS3EnvironmentValue("AWS_ACCESS_KEY_ID") ||
       secret_key != LanceS3EnvironmentValue("AWS_SECRET_ACCESS_KEY"))) {
    return true;
  }

  string session_token;
#ifdef LANCE_VANE_DISTRIBUTED
  auto session_token_scope = SettingScope::INVALID;
  auto has_session_token = TryGetLanceS3Setting(
      context, "s3_session_token", session_token, &session_token_scope, true);
  if (has_session_token &&
      (session_token_scope == SettingScope::LOCAL ||
       session_token != LanceS3EnvironmentValue("AWS_SESSION_TOKEN"))) {
    return true;
  }
#else
  if (TryGetLanceS3Setting(context, "s3_session_token", session_token) &&
      session_token != LanceS3EnvironmentValue("AWS_SESSION_TOKEN")) {
    return true;
  }
#endif

  string region;
  if (TryGetLanceS3Setting(context, "s3_region", region)) {
    auto environment_region = LanceS3EnvironmentValue("AWS_REGION");
    if (environment_region.empty()) {
      environment_region = LanceS3EnvironmentValue("AWS_DEFAULT_REGION");
    }
    if (region != environment_region) {
      return true;
    }
  }

  string url_style;
  if (TryGetLanceS3Setting(context, "s3_url_style", url_style) &&
      !StringUtil::CIEquals(url_style, "vhost")) {
    return true;
  }

  string use_ssl;
  return TryGetLanceS3Setting(context, "s3_use_ssl", use_ssl) &&
         !StringUtil::CIEquals(use_ssl, "true");
}

static void AppendLanceStorageOption(vector<string> &out_keys,
                                     vector<string> &out_values,
                                     const string &key, const string &value) {
  if (value.empty()) {
    return;
  }
  out_keys.push_back(key);
  out_values.push_back(value);
}

static bool IsLanceS3StorageOptionAlias(const string &key) {
  return StringUtil::CIEquals(key, "access_key_id") ||
         StringUtil::CIEquals(key, "secret_access_key") ||
         StringUtil::CIEquals(key, "session_token") ||
         StringUtil::CIEquals(key, "region") ||
         StringUtil::CIEquals(key, "endpoint") ||
         StringUtil::CIEquals(key, "virtual_hosted_style_request");
}

static string CanonicalLanceS3StorageOptionKey(const string &key) {
  if (StringUtil::CIEquals(key, "access_key_id") ||
      StringUtil::CIEquals(key, "aws_access_key_id")) {
    return "aws_access_key_id";
  }
  if (StringUtil::CIEquals(key, "secret_access_key") ||
      StringUtil::CIEquals(key, "aws_secret_access_key")) {
    return "aws_secret_access_key";
  }
  if (StringUtil::CIEquals(key, "session_token") ||
      StringUtil::CIEquals(key, "aws_session_token")) {
    return "aws_session_token";
  }
  if (StringUtil::CIEquals(key, "region") ||
      StringUtil::CIEquals(key, "aws_region")) {
    return "aws_region";
  }
  if (StringUtil::CIEquals(key, "endpoint") ||
      StringUtil::CIEquals(key, "aws_endpoint")) {
    return "aws_endpoint";
  }
  if (StringUtil::CIEquals(key, "virtual_hosted_style_request") ||
      StringUtil::CIEquals(key, "aws_virtual_hosted_style_request")) {
    return "aws_virtual_hosted_style_request";
  }
  return key;
}

static void CanonicalizeLanceS3StorageOptions(vector<string> &out_keys,
                                              vector<string> &out_values) {
  vector<string> normalized_keys;
  vector<string> normalized_values;
  normalized_keys.reserve(out_keys.size());
  normalized_values.reserve(out_values.size());

  for (idx_t i = 0; i < out_keys.size(); i++) {
    auto canonical_key = CanonicalLanceS3StorageOptionKey(out_keys[i]);
    if (IsLanceS3StorageOptionAlias(out_keys[i])) {
      auto has_explicit_canonical = false;
      for (auto &candidate : out_keys) {
        if (!IsLanceS3StorageOptionAlias(candidate) &&
            StringUtil::CIEquals(candidate, canonical_key)) {
          has_explicit_canonical = true;
          break;
        }
      }
      if (has_explicit_canonical) {
        continue;
      }
    }
    normalized_keys.push_back(std::move(canonical_key));
    normalized_values.push_back(std::move(out_values[i]));
  }

  out_keys = std::move(normalized_keys);
  out_values = std::move(normalized_values);
}

static void
FillLanceStorageOptionsFromDuckDBS3Settings(ClientContext &context,
                                            vector<string> &out_keys,
                                            vector<string> &out_values) {
  string access_key_id;
  string secret_access_key;
  string session_token;
  string region;
  auto access_key_scope = SettingScope::INVALID;
  auto secret_key_scope = SettingScope::INVALID;
  auto session_token_scope = SettingScope::INVALID;
  auto has_access_key = TryGetLanceS3Setting(context, "s3_access_key_id",
                                             access_key_id, &access_key_scope);
  auto has_secret_key = TryGetLanceS3Setting(
      context, "s3_secret_access_key", secret_access_key, &secret_key_scope);
#ifdef LANCE_VANE_DISTRIBUTED
  auto has_session_token = TryGetLanceS3Setting(
      context, "s3_session_token", session_token, &session_token_scope, true);
  auto has_local_access_key = access_key_scope == SettingScope::LOCAL;
  auto has_local_secret_key = secret_key_scope == SettingScope::LOCAL;
  if (has_local_access_key != has_local_secret_key) {
    throw InvalidInputException(
        "Connection-local S3 credentials must set both s3_access_key_id and "
        "s3_secret_access_key");
  }
  if (has_local_access_key && (!has_access_key || !has_secret_key)) {
    throw InvalidInputException(
        "Connection-local S3 access and secret keys must be non-empty");
  }
#else
  auto has_session_token = TryGetLanceS3Setting(
      context, "s3_session_token", session_token, &session_token_scope);
#endif
  TryGetLanceS3Setting(context, "s3_region", region);
  AppendLanceStorageOption(out_keys, out_values, "aws_access_key_id",
                           access_key_id);
  AppendLanceStorageOption(out_keys, out_values, "aws_secret_access_key",
                           secret_access_key);
  // DuckDB exposes AWS environment variables as GLOBAL settings. Do not merge
  // an inherited session token into a connection-local static credential pair.
  // Vane session replay can promote those values to LOCAL on a worker, so also
  // recognize the inherited token by value when the static pair overrides the
  // process credentials. A genuinely explicit token is still forwarded.
  auto process_access_key = LanceS3EnvironmentValue("AWS_ACCESS_KEY_ID");
  auto process_secret_key = LanceS3EnvironmentValue("AWS_SECRET_ACCESS_KEY");
  auto process_session_token = LanceS3EnvironmentValue("AWS_SESSION_TOKEN");
  auto static_credentials_override_process_environment =
      has_access_key && has_secret_key &&
      (access_key_id != process_access_key ||
       secret_access_key != process_secret_key);
  auto token_belongs_to_process_environment =
      has_access_key && has_secret_key && has_session_token &&
      ((access_key_scope == SettingScope::LOCAL &&
        secret_key_scope == SettingScope::LOCAL &&
        session_token_scope == SettingScope::GLOBAL) ||
       (static_credentials_override_process_environment &&
        !process_session_token.empty() &&
        session_token == process_session_token));
#ifdef LANCE_VANE_DISTRIBUTED
  if (has_session_token && !token_belongs_to_process_environment) {
    if (session_token_scope == SettingScope::LOCAL) {
      // Presence is meaningful even when the value is empty: it explicitly
      // clears an inherited process session token for static credentials.
      out_keys.push_back("aws_session_token");
      out_values.push_back(session_token);
    } else {
      AppendLanceStorageOption(out_keys, out_values, "aws_session_token",
                               session_token);
    }
  }
#else
  if (!token_belongs_to_process_environment) {
    AppendLanceStorageOption(out_keys, out_values, "aws_session_token",
                             session_token);
  }
#endif
  AppendLanceStorageOption(out_keys, out_values, "aws_region", region);

  string endpoint;
  if (TryGetLanceS3Setting(context, "s3_endpoint", endpoint)) {
    Value use_ssl_value;
    auto use_ssl = true;
    if (context.TryGetCurrentSetting("s3_use_ssl", use_ssl_value) &&
        !use_ssl_value.IsNull()) {
      use_ssl =
          use_ssl_value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
    }
    if (endpoint.find("://") == string::npos) {
      endpoint = string(use_ssl ? "https://" : "http://") + endpoint;
    }
    AppendLanceStorageOption(out_keys, out_values, "aws_endpoint", endpoint);
    if (StringUtil::StartsWith(StringUtil::Lower(endpoint), "http://")) {
      AppendLanceStorageOption(out_keys, out_values, "allow_http", "true");
    }
  }

  string url_style;
  if (TryGetLanceS3Setting(context, "s3_url_style", url_style)) {
    if (StringUtil::CIEquals(url_style, "path")) {
      AppendLanceStorageOption(out_keys, out_values,
                               "aws_virtual_hosted_style_request", "false");
    } else if (StringUtil::CIEquals(url_style, "vhost")) {
      AppendLanceStorageOption(out_keys, out_values,
                               "aws_virtual_hosted_style_request", "true");
    }
  }
}
#endif

static void FillLanceNamespaceAuthFromSecrets(ClientContext &context,
                                              const string &endpoint,
                                              string &out_bearer_token,
                                              string &out_api_key) {
  out_bearer_token.clear();
  out_api_key.clear();

  auto &secret_manager = SecretManager::Get(context);
  auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
#ifdef LANCE_VANE_DISTRIBUTED
  auto lookup_endpoint = LanceVaneCanonicalizeSecretScope(endpoint);
  auto secret_match = secret_manager.LookupSecret(transaction, lookup_endpoint,
                                                  "lance_namespace");
#else
  auto secret_match =
      secret_manager.LookupSecret(transaction, endpoint, "lance_namespace");
#endif
  if (!secret_match.HasMatch() || !secret_match.secret_entry ||
      !secret_match.secret_entry->secret) {
    return;
  }

  auto *kv_secret = dynamic_cast<const KeyValueSecret *>(
      secret_match.secret_entry->secret.get());
  if (!kv_secret) {
    return;
  }

#ifdef LANCE_VANE_DISTRIBUTED
  auto token = SecretValueToString(kv_secret->TryGetValue("token"));
  auto bearer_token =
      SecretValueToString(kv_secret->TryGetValue("bearer_token"));
  out_bearer_token = std::move(token);
  if (!bearer_token.empty()) {
    out_bearer_token = std::move(bearer_token);
  }
#else
  out_bearer_token = SecretValueToString(kv_secret->TryGetValue("token"));
  if (out_bearer_token.empty()) {
    out_bearer_token =
        SecretValueToString(kv_secret->TryGetValue("bearer_token"));
  }
#endif
  out_api_key = SecretValueToString(kv_secret->TryGetValue("api_key"));
}

static void ApplyAuthOverrideValue(const Value &value, string &out) {
  if (value.IsNull()) {
    return;
  }
  auto s = value.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
  if (!s.empty()) {
    out = std::move(s);
  }
}

void ResolveLanceNamespaceAuth(ClientContext &context, const string &endpoint,
                               const unordered_map<string, Value> &options,
                               string &out_bearer_token, string &out_api_key) {
  FillLanceNamespaceAuthFromSecrets(context, endpoint, out_bearer_token,
                                    out_api_key);
  ResolveLanceNamespaceAuthOverrides(options, out_bearer_token, out_api_key);
}

void ResolveLanceNamespaceAuth(ClientContext &context, const string &endpoint,
                               const named_parameter_map_t &options,
                               string &out_bearer_token, string &out_api_key) {
  FillLanceNamespaceAuthFromSecrets(context, endpoint, out_bearer_token,
                                    out_api_key);
  auto token_it = options.find("token");
  if (token_it != options.end()) {
    ApplyAuthOverrideValue(token_it->second, out_bearer_token);
  }
  auto bearer_it = options.find("bearer_token");
  if (bearer_it != options.end()) {
    ApplyAuthOverrideValue(bearer_it->second, out_bearer_token);
  }
  auto key_it = options.find("api_key");
  if (key_it != options.end()) {
    ApplyAuthOverrideValue(key_it->second, out_api_key);
  }
}

void ResolveLanceNamespaceAuthOverrides(
    const unordered_map<string, Value> &options, string &out_bearer_token,
    string &out_api_key) {
#ifdef LANCE_VANE_DISTRIBUTED
  // Resolve aliases in a fixed order. BEARER_TOKEN is the canonical spelling
  // and wins over TOKEN regardless of unordered-map iteration order.
  const Value *token = nullptr;
  const Value *bearer_token = nullptr;
  const Value *api_key = nullptr;
  for (auto &kv : options) {
    if (StringUtil::CIEquals(kv.first, "token")) {
      token = &kv.second;
    } else if (StringUtil::CIEquals(kv.first, "bearer_token")) {
      bearer_token = &kv.second;
    } else if (StringUtil::CIEquals(kv.first, "api_key")) {
      api_key = &kv.second;
    }
  }
  if (token) {
    ApplyAuthOverrideValue(*token, out_bearer_token);
  }
  if (bearer_token) {
    ApplyAuthOverrideValue(*bearer_token, out_bearer_token);
  }
  if (api_key) {
    ApplyAuthOverrideValue(*api_key, out_api_key);
  }
#else
  for (auto &kv : options) {
    if (StringUtil::CIEquals(kv.first, "token")) {
      ApplyAuthOverrideValue(kv.second, out_bearer_token);
      continue;
    }
    if (StringUtil::CIEquals(kv.first, "bearer_token")) {
      ApplyAuthOverrideValue(kv.second, out_bearer_token);
      continue;
    }
    if (StringUtil::CIEquals(kv.first, "api_key")) {
      ApplyAuthOverrideValue(kv.second, out_api_key);
      continue;
    }
  }
#endif
}

void ResolveLanceStorageOptions(ClientContext &context, const string &path,
                                string &out_open_path, vector<string> &out_keys,
                                vector<string> &out_values) {
  out_open_path = path;
  out_keys.clear();
  out_values.clear();

  out_open_path = LanceNormalizeS3Scheme(out_open_path);
  LanceFillStorageOptionsFromSecrets(context, out_open_path, out_keys,
                                     out_values);
}

#ifdef LANCE_VANE_DISTRIBUTED
bool LanceHasMatchingStorageSecret(ClientContext &context, const string &path) {
  auto normalized_path = LanceVaneCanonicalizeSecretScope(path);
  auto &secret_manager = SecretManager::Get(context);
  auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
  auto secret_match =
      secret_manager.LookupSecret(transaction, normalized_path, "lance");
  if (!secret_match.HasMatch() || !secret_match.secret_entry ||
      !secret_match.secret_entry->secret) {
    return false;
  }

  auto *kv_secret = dynamic_cast<const KeyValueSecret *>(
      secret_match.secret_entry->secret.get());
  if (!kv_secret) {
    return false;
  }
  for (auto &entry : kv_secret->secret_map) {
    // Match the option-presence rule used by
    // LanceFillStorageOptionsFromSecrets. Do not stringify secret values merely
    // to decide distributed eligibility.
    if (!entry.second.IsNull()) {
      return true;
    }
  }
  return false;
}

void ResolveLanceStorageOptionsForDistributedRead(ClientContext &context,
                                                  const string &path,
                                                  string &out_open_path,
                                                  vector<string> &out_keys,
                                                  vector<string> &out_values) {
  ResolveLanceStorageOptions(context, path, out_open_path, out_keys,
                             out_values);
  if (!StringUtil::StartsWith(out_open_path, "s3://")) {
    return;
  }
  CanonicalizeLanceS3StorageOptions(out_keys, out_values);
  if (out_keys.empty()) {
    if (ProcessHasLanceS3Environment() &&
        !LanceS3SettingsOverrideProcessEnvironment(context)) {
      // The source connection can still use Lance's ordinary AWS environment
      // resolution. Avoid replacing that provider chain with DuckDB's default,
      // partially populated s3_* settings. Vane removes these variables from
      // isolated workers, where the captured settings below are authoritative.
      return;
    }
    // Vane scrubs inherited AWS environment variables from shared workers and
    // replays the query session through DuckDB's s3_* settings instead.
    FillLanceStorageOptionsFromDuckDBS3Settings(context, out_keys, out_values);
  }
}

bool LanceVaneDirectoryNamespaceSessionMatches(
    ClientContext &context, const LanceNamespaceTableConfig &config) {
  string current_root;
  vector<string> current_keys;
  vector<string> current_values;
  ResolveLanceStorageOptionsForDistributedRead(
      context, config.root, current_root, current_keys, current_values);
  if (current_root != config.root ||
      current_keys.size() != current_values.size() ||
      config.option_keys.size() != config.option_values.size()) {
    return false;
  }

  vector<pair<string, string>> current_options;
  vector<pair<string, string>> attached_options;
  current_options.reserve(current_keys.size());
  attached_options.reserve(config.option_keys.size());
  for (idx_t option_idx = 0; option_idx < current_keys.size(); option_idx++) {
    current_options.emplace_back(current_keys[option_idx],
                                 current_values[option_idx]);
  }
  for (idx_t option_idx = 0; option_idx < config.option_keys.size();
       option_idx++) {
    attached_options.emplace_back(config.option_keys[option_idx],
                                  config.option_values[option_idx]);
  }
  std::sort(current_options.begin(), current_options.end());
  std::sort(attached_options.begin(), attached_options.end());
  return current_options == attached_options;
}
#endif

void BuildStorageOptionPointerArrays(const vector<string> &option_keys,
                                     const vector<string> &option_values,
                                     vector<const char *> &out_key_ptrs,
                                     vector<const char *> &out_value_ptrs) {
  if (option_keys.size() != option_values.size()) {
    throw InternalException(
        "Storage option keys/values size mismatch for Lance");
  }
  out_key_ptrs.clear();
  out_value_ptrs.clear();
  out_key_ptrs.reserve(option_keys.size());
  out_value_ptrs.reserve(option_values.size());
  for (idx_t i = 0; i < option_keys.size(); i++) {
    out_key_ptrs.push_back(option_keys[i].c_str());
    out_value_ptrs.push_back(option_values[i].c_str());
  }
}

#ifdef LANCE_VANE_DISTRIBUTED
static string LanceVaneNamespaceError(const string &endpoint,
                                      const string &bearer_token,
                                      const string &api_key,
                                      const string &headers_tsv) {
  (void)endpoint;
  (void)bearer_token;
  (void)api_key;
  (void)headers_tsv;
  auto error = LanceConsumeLastError();
  // A namespace implementation can include opaque storage credentials or a
  // presigned URL in any remote error body, even when the public endpoint was
  // contacted without client-side authentication. Vane must therefore treat
  // all REST namespace backend diagnostics as private.
  return error.empty() ? "unknown error" : "details redacted";
}

static string
LanceVaneDirectoryNamespaceError(const string &root,
                                 const vector<string> &option_keys) {
  auto error = LanceConsumeLastError();
  if (!option_keys.empty() || LanceVanePathHasPrivateUriComponents(root) ||
      LanceVanePathIsRemote(root)) {
    return error.empty() ? "unknown error" : "details redacted";
  }
  return error.empty() ? "unknown error" : error;
}
#endif

bool TryLanceNamespaceListTables(
    ClientContext &context, const string &endpoint, const string &namespace_id,
    const string &bearer_token, const string &api_key, const string &delimiter,
    const string &headers_tsv, vector<string> &out_tables, string &out_error) {
  out_tables.clear();
  out_error.clear();

  const char *bearer_ptr =
      bearer_token.empty() ? nullptr : bearer_token.c_str();
  const char *api_key_ptr = api_key.empty() ? nullptr : api_key.c_str();
  const char *delimiter_ptr = delimiter.empty() ? nullptr : delimiter.c_str();
  const char *headers_ptr = headers_tsv.empty() ? nullptr : headers_tsv.c_str();

  auto *ptr = lance_namespace_list_tables(
      endpoint.c_str(), namespace_id.c_str(), bearer_ptr, api_key_ptr,
      delimiter_ptr, headers_ptr);
  if (!ptr) {
#ifdef LANCE_VANE_DISTRIBUTED
    out_error =
        LanceVaneNamespaceError(endpoint, bearer_token, api_key, headers_tsv);
#else
    out_error = LanceConsumeLastError();
    if (out_error.empty()) {
      out_error = "unknown error";
    }
#endif
    return false;
  }
  string joined = ptr;
  lance_free_string(ptr);

  vector<string> parts = StringUtil::Split(joined, '\n');
  for (auto &p : parts) {
    if (!p.empty()) {
      out_tables.push_back(std::move(p));
    }
  }
  return true;
}

static void ParseStorageOptionsTsv(const char *ptr, vector<string> &out_keys,
                                   vector<string> &out_values) {
  out_keys.clear();
  out_values.clear();
  if (!ptr) {
    return;
  }
  string joined = ptr;
  lance_free_string(ptr);

  for (auto &line : StringUtil::Split(joined, '\n')) {
    if (line.empty()) {
      continue;
    }
    auto parts = StringUtil::Split(line, '\t');
    if (parts.size() != 2) {
      continue;
    }
    out_keys.push_back(std::move(parts[0]));
    out_values.push_back(std::move(parts[1]));
  }
}

bool TryLanceNamespaceDescribeTable(
    ClientContext &context, const string &endpoint, const string &table_id,
    const string &bearer_token, const string &api_key, const string &delimiter,
    const string &headers_tsv, string &out_location,
    vector<string> &out_option_keys, vector<string> &out_option_values,
    string &out_error) {
  (void)context;
  out_location.clear();
  out_option_keys.clear();
  out_option_values.clear();
  out_error.clear();

  const char *bearer_ptr =
      bearer_token.empty() ? nullptr : bearer_token.c_str();
  const char *api_key_ptr = api_key.empty() ? nullptr : api_key.c_str();
  const char *delimiter_ptr = delimiter.empty() ? nullptr : delimiter.c_str();
  const char *headers_ptr = headers_tsv.empty() ? nullptr : headers_tsv.c_str();

  const char *location_ptr = nullptr;
  const char *options_ptr = nullptr;
  auto rc = lance_namespace_describe_table(
      endpoint.c_str(), table_id.c_str(), bearer_ptr, api_key_ptr,
      delimiter_ptr, headers_ptr, &location_ptr, &options_ptr);
  if (rc != 0) {
#ifdef LANCE_VANE_DISTRIBUTED
    out_error =
        LanceVaneNamespaceError(endpoint, bearer_token, api_key, headers_tsv);
#else
    out_error = LanceConsumeLastError();
    if (out_error.empty()) {
      out_error = "unknown error";
    }
#endif
    return false;
  }
  if (location_ptr) {
    out_location = location_ptr;
    lance_free_string(location_ptr);
  }
  ParseStorageOptionsTsv(options_ptr, out_option_keys, out_option_values);
  return true;
}

bool TryLanceNamespaceCreateEmptyTable(
    ClientContext &context, const string &endpoint, const string &table_id,
    const string &bearer_token, const string &api_key, const string &delimiter,
    const string &headers_tsv, string &out_location,
    vector<string> &out_option_keys, vector<string> &out_option_values,
    string &out_error) {
  (void)context;
  out_location.clear();
  out_option_keys.clear();
  out_option_values.clear();
  out_error.clear();

  const char *bearer_ptr =
      bearer_token.empty() ? nullptr : bearer_token.c_str();
  const char *api_key_ptr = api_key.empty() ? nullptr : api_key.c_str();
  const char *delimiter_ptr = delimiter.empty() ? nullptr : delimiter.c_str();
  const char *headers_ptr = headers_tsv.empty() ? nullptr : headers_tsv.c_str();

  const char *location_ptr = nullptr;
  const char *options_ptr = nullptr;
  auto rc = lance_namespace_create_empty_table(
      endpoint.c_str(), table_id.c_str(), bearer_ptr, api_key_ptr,
      delimiter_ptr, headers_ptr, &location_ptr, &options_ptr);
  if (rc != 0) {
    out_error = LanceConsumeLastError();
    if (out_error.empty()) {
      out_error = "unknown error";
    }
    return false;
  }
  if (location_ptr) {
    out_location = location_ptr;
    lance_free_string(location_ptr);
  }
  ParseStorageOptionsTsv(options_ptr, out_option_keys, out_option_values);
  return true;
}

bool TryLanceNamespaceDropTable(ClientContext &context, const string &endpoint,
                                const string &table_id,
                                const string &bearer_token,
                                const string &api_key, const string &delimiter,
                                const string &headers_tsv, string &out_error) {
  (void)context;
  out_error.clear();

  const char *bearer_ptr =
      bearer_token.empty() ? nullptr : bearer_token.c_str();
  const char *api_key_ptr = api_key.empty() ? nullptr : api_key.c_str();
  const char *delimiter_ptr = delimiter.empty() ? nullptr : delimiter.c_str();
  const char *headers_ptr = headers_tsv.empty() ? nullptr : headers_tsv.c_str();

  auto rc =
      lance_namespace_drop_table(endpoint.c_str(), table_id.c_str(), bearer_ptr,
                                 api_key_ptr, delimiter_ptr, headers_ptr);
  if (rc != 0) {
    out_error = LanceConsumeLastError();
    if (out_error.empty()) {
      out_error = "unknown error";
    }
    return false;
  }
  return true;
}

bool TryLanceDirNamespaceListTables(ClientContext &context, const string &root,
                                    vector<string> &out_tables,
                                    string &out_error) {
  out_tables.clear();
  out_error.clear();

  string open_root;
  vector<string> option_keys;
  vector<string> option_values;
#ifdef LANCE_VANE_DISTRIBUTED
  ResolveLanceStorageOptionsForDistributedRead(context, root, open_root,
                                               option_keys, option_values);
#else
  ResolveLanceStorageOptions(context, root, open_root, option_keys,
                             option_values);
#endif

  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                  value_ptrs);

  auto *ptr = lance_dir_namespace_list_tables(
      open_root.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
      value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size());
  if (!ptr) {
#ifdef LANCE_VANE_DISTRIBUTED
    out_error = LanceVaneDirectoryNamespaceError(root, option_keys);
#else
    out_error = LanceConsumeLastError();
    if (out_error.empty()) {
      out_error = "unknown error";
    }
#endif
    return false;
  }

  string joined = ptr;
  lance_free_string(ptr);

  vector<string> parts = StringUtil::Split(joined, '\n');
  for (auto &p : parts) {
    if (!p.empty()) {
      out_tables.push_back(std::move(p));
    }
  }
  return true;
}

void *
LanceOpenDatasetInNamespace(ClientContext &context, const string &endpoint,
                            const string &table_id, const string &bearer_token,
                            const string &api_key, const string &delimiter,
                            const string &headers_tsv, string &out_table_uri) {
  out_table_uri.clear();
  auto *session = LanceGetSessionHandle(context);
  const char *bearer_ptr =
      bearer_token.empty() ? nullptr : bearer_token.c_str();
  const char *api_key_ptr = api_key.empty() ? nullptr : api_key.c_str();
  const char *delimiter_ptr = delimiter.empty() ? nullptr : delimiter.c_str();
  const char *headers_ptr = headers_tsv.empty() ? nullptr : headers_tsv.c_str();

  const char *uri_ptr = nullptr;
  auto *dataset = lance_open_dataset_in_namespace_with_session(
      endpoint.c_str(), table_id.c_str(), bearer_ptr, api_key_ptr,
      delimiter_ptr, headers_ptr, session, &uri_ptr);
  if (uri_ptr) {
    out_table_uri = uri_ptr;
    lance_free_string(uri_ptr);
  }
  return dataset;
}

void *LanceOpenDataset(ClientContext &context, const string &path) {
  string open_path;
  vector<string> option_keys;
  vector<string> option_values;
  ResolveLanceStorageOptions(context, path, open_path, option_keys,
                             option_values);
  auto *session = LanceGetSessionHandle(context);

  if (option_keys.empty()) {
    return lance_open_dataset_with_session(open_path.c_str(), session);
  }

  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                  value_ptrs);
  return lance_open_dataset_with_storage_options_and_session(
      open_path.c_str(), key_ptrs.data(), value_ptrs.data(), option_keys.size(),
      session);
}

#ifdef LANCE_VANE_DISTRIBUTED
void *LanceOpenDatasetForDistributedScan(ClientContext &context,
                                         const string &path) {
  string open_path;
  vector<string> option_keys;
  vector<string> option_values;
  ResolveLanceStorageOptionsForDistributedRead(context, path, open_path,
                                               option_keys, option_values);
  auto *session = LanceGetSessionHandle(context);

  if (option_keys.empty()) {
    return lance_open_dataset_with_session(open_path.c_str(), session);
  }

  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                  value_ptrs);
  return lance_open_dataset_with_storage_options_and_session(
      open_path.c_str(), key_ptrs.data(), value_ptrs.data(), option_keys.size(),
      session);
}

void *LanceOpenDatasetVersionForDistributedScan(ClientContext &context,
                                                const string &path,
                                                uint64_t version) {
  string open_path;
  vector<string> option_keys;
  vector<string> option_values;
  ResolveLanceStorageOptionsForDistributedRead(context, path, open_path,
                                               option_keys, option_values);
  auto *session = LanceGetSessionHandle(context);

  if (option_keys.empty()) {
    return lance_vane_open_dataset_version_with_session(open_path.c_str(),
                                                        version, session);
  }

  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                  value_ptrs);
  return lance_vane_open_dataset_version_with_storage_options_and_session(
      open_path.c_str(), version, key_ptrs.data(), value_ptrs.data(),
      option_keys.size(), session);
}
#endif

static unordered_map<string, Value>
BuildNamespaceAuthOverrideOptions(const string &bearer_token_override,
                                  const string &api_key_override) {
  unordered_map<string, Value> options;
  if (!bearer_token_override.empty()) {
    options["bearer_token"] = Value(bearer_token_override);
  }
  if (!api_key_override.empty()) {
    options["api_key"] = Value(api_key_override);
  }
  return options;
}

LanceTableEntry *TryResolveLanceTableEntry(ClientContext &context,
                                           const string &input) {
  // Fast-path bail-out: obvious filesystem / URL literals can never match a
  // qualified catalog identifier.  Avoids parsing attempts and potential
  // secondary lookups that would just end up throwing ParserException.
  if (input.empty() || input.find('/') != string::npos ||
      input.find('\\') != string::npos || input.find("://") != string::npos) {
    return nullptr;
  }

  QualifiedName qname;
  try {
    qname = QualifiedName::Parse(input);
  } catch (ParserException &) {
    return nullptr;
  }

  EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, qname.name);
  auto entry = Catalog::GetEntry(context, qname.catalog, qname.schema,
                                 lookup_info, OnEntryNotFound::RETURN_NULL);
  if (!entry) {
    return nullptr;
  }
  auto *table_entry = dynamic_cast<TableCatalogEntry *>(entry.get());
  if (!table_entry) {
    return nullptr;
  }
  return dynamic_cast<LanceTableEntry *>(table_entry);
}

void *LanceOpenDatasetForTable(ClientContext &context,
                               const LanceTableEntry &table,
                               string &out_display_uri) {
  out_display_uri = table.DatasetUri();
  if (!table.IsNamespaceBacked()) {
    return LanceOpenDataset(context, table.DatasetUri());
  }

  auto &cfg = table.NamespaceConfig();
  if (cfg.IsDirectory()) {
    vector<const char *> key_ptrs;
    vector<const char *> value_ptrs;
    BuildStorageOptionPointerArrays(cfg.option_keys, cfg.option_values,
                                    key_ptrs, value_ptrs);

    const char *uri_ptr = nullptr;
    auto *dataset = lance_open_dataset_in_dir_namespace_with_session(
        cfg.root.c_str(), cfg.table_id.c_str(),
        key_ptrs.empty() ? nullptr : key_ptrs.data(),
        value_ptrs.empty() ? nullptr : value_ptrs.data(),
        cfg.option_keys.size(), LanceGetSessionHandle(context), &uri_ptr);
    if (uri_ptr) {
      out_display_uri = uri_ptr;
      lance_free_string(uri_ptr);
    } else if (!cfg.display_uri.empty()) {
      out_display_uri = cfg.display_uri;
    } else {
      out_display_uri = LanceDirectoryNamespaceDatasetUri(cfg);
    }
    return dataset;
  }

  unordered_map<string, Value> overrides = BuildNamespaceAuthOverrideOptions(
      cfg.bearer_token_override, cfg.api_key_override);
  string bearer_token;
  string api_key;
  ResolveLanceNamespaceAuth(context, cfg.endpoint, overrides, bearer_token,
                            api_key);

  string table_uri;
  auto *dataset = LanceOpenDatasetInNamespace(
      context, cfg.endpoint, cfg.table_id, bearer_token, api_key, cfg.delimiter,
      cfg.headers_tsv, table_uri);
  if (!table_uri.empty()) {
    out_display_uri = table_uri;
  } else {
    out_display_uri = cfg.endpoint + "/" + cfg.table_id;
  }
  return dataset;
}

void ResolveLanceStorageOptionsForTable(ClientContext &context,
                                        const LanceTableEntry &table,
                                        string &out_open_path,
                                        vector<string> &out_option_keys,
                                        vector<string> &out_option_values,
                                        string &out_display_uri) {
  out_display_uri = table.DatasetUri();
  if (!table.IsNamespaceBacked()) {
    ResolveLanceStorageOptions(context, table.DatasetUri(), out_open_path,
                               out_option_keys, out_option_values);
    return;
  }

  auto &cfg = table.NamespaceConfig();
  if (cfg.IsDirectory()) {
    out_display_uri = LanceDirectoryNamespaceDatasetUri(cfg);
    out_open_path = out_display_uri;
    out_option_keys = cfg.option_keys;
    out_option_values = cfg.option_values;
    return;
  }

  unordered_map<string, Value> overrides = BuildNamespaceAuthOverrideOptions(
      cfg.bearer_token_override, cfg.api_key_override);
  string bearer_token;
  string api_key;
  ResolveLanceNamespaceAuth(context, cfg.endpoint, overrides, bearer_token,
                            api_key);

  string location;
  string error;
  vector<string> option_keys;
  vector<string> option_values;
  if (!TryLanceNamespaceDescribeTable(context, cfg.endpoint, cfg.table_id,
                                      bearer_token, api_key, cfg.delimiter,
                                      cfg.headers_tsv, location, option_keys,
                                      option_values, error)) {
    throw IOException("Failed to describe Lance table via namespace: " +
                      (error.empty() ? "unknown error" : error));
  }

  out_display_uri =
      location.empty() ? (cfg.endpoint + "/" + cfg.table_id) : location;

  if (!option_keys.empty()) {
    out_open_path = LanceNormalizeS3Scheme(location);
    out_option_keys = std::move(option_keys);
    out_option_values = std::move(option_values);
    return;
  }

  ResolveLanceStorageOptions(context, location, out_open_path, out_option_keys,
                             out_option_values);
}

int64_t LanceTruncateDatasetWithStorageOptions(
    ClientContext &context, const string &open_path,
    const vector<string> &option_keys, const vector<string> &option_values,
    const string &display_uri) {
  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                  value_ptrs);

  void *dataset = nullptr;
  if (option_keys.empty()) {
    dataset = lance_open_dataset(open_path.c_str());
  } else {
    dataset = lance_open_dataset_with_storage_options(
        open_path.c_str(), key_ptrs.data(), value_ptrs.data(),
        option_keys.size());
  }
  if (!dataset) {
    throw IOException("Failed to open Lance dataset: " + display_uri +
                      LanceFormatErrorSuffix());
  }

  auto row_count = lance_dataset_count_rows(dataset);
  if (row_count < 0) {
    lance_close_dataset(dataset);
    throw IOException("Failed to count rows from Lance dataset: " +
                      display_uri + LanceFormatErrorSuffix());
  }

  auto *schema_handle = lance_get_schema(dataset);
  if (!schema_handle) {
    lance_close_dataset(dataset);
    throw IOException("Failed to get schema from Lance dataset: " +
                      display_uri + LanceFormatErrorSuffix());
  }

  ArrowSchemaWrapper schema_root;
  memset(&schema_root.arrow_schema, 0, sizeof(schema_root.arrow_schema));
  if (lance_schema_to_arrow(schema_handle, &schema_root.arrow_schema) != 0) {
    lance_free_schema(schema_handle);
    lance_close_dataset(dataset);
    throw IOException(
        "Failed to export Lance schema to Arrow C Data Interface" +
        LanceFormatErrorSuffix());
  }

  lance_free_schema(schema_handle);
  lance_close_dataset(dataset);

  auto *writer = lance_open_writer_with_storage_options(
      open_path.c_str(), "overwrite",
      key_ptrs.empty() ? nullptr : key_ptrs.data(),
      value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
      LANCE_DEFAULT_MAX_ROWS_PER_FILE, LANCE_DEFAULT_MAX_ROWS_PER_GROUP,
      LANCE_DEFAULT_MAX_BYTES_PER_FILE, nullptr, LanceGetSessionHandle(context),
      &schema_root.arrow_schema);
  if (!writer) {
    throw IOException("Failed to open Lance writer: " + open_path +
                      LanceFormatErrorSuffix());
  }
  auto rc = lance_writer_finish(writer);
  lance_close_writer(writer);
  if (rc != 0) {
    throw IOException("Failed to finalize Lance dataset write" +
                      LanceFormatErrorSuffix());
  }

  return row_count;
}

int64_t LanceTruncateDataset(ClientContext &context,
                             const string &dataset_uri) {
  string open_path;
  vector<string> option_keys;
  vector<string> option_values;
  ResolveLanceStorageOptions(context, dataset_uri, open_path, option_keys,
                             option_values);
  return LanceTruncateDatasetWithStorageOptions(context, open_path, option_keys,
                                                option_values, dataset_uri);
}

void ApplyDuckDBFilters(ClientContext &context, TableFilterSet &filters,
                        DataChunk &chunk, SelectionVector &sel) {
  if (chunk.size() == 0) {
    return;
  }
  unique_ptr<Expression> combined;
  for (auto &it : filters.filters) {
    auto scan_col_idx = it.first;
    if (scan_col_idx >= chunk.ColumnCount()) {
      continue;
    }
    BoundReferenceExpression col_expr(chunk.data[scan_col_idx].GetType(),
                                      NumericCast<storage_t>(scan_col_idx));
    auto expr = it.second->ToExpression(col_expr);
    if (!combined) {
      combined = std::move(expr);
    } else {
      auto conj = make_uniq<BoundConjunctionExpression>(
          ExpressionType::CONJUNCTION_AND);
      conj->children.push_back(std::move(combined));
      conj->children.push_back(std::move(expr));
      combined = std::move(conj);
    }
  }
  if (!combined) {
    return;
  }
  ExpressionExecutor executor(context, *combined);
  auto selected = executor.SelectExpression(chunk, sel);
  if (selected != chunk.size()) {
    chunk.Slice(sel, selected);
  }
}

} // namespace duckdb
