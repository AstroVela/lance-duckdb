#pragma once

#include "duckdb.hpp"

struct LanceNamespaceQueryConfig;

namespace duckdb {

string LanceConsumeLastError();
string LanceFormatErrorSuffix();

bool IsComputedSearchColumn(const string &name);

void ApplyDuckDBFilters(ClientContext &context, TableFilterSet &filters,
                        DataChunk &chunk, SelectionVector &sel);

void *LanceOpenDataset(ClientContext &context, const string &path);
#ifdef LANCE_VANE_DISTRIBUTED
void *LanceOpenDatasetForDistributedScan(ClientContext &context,
                                         const string &path);
void *LanceOpenDatasetVersionForDistributedScan(ClientContext &context,
                                                const string &path,
                                                uint64_t version);
void *LanceOpenDatasetVersionFromManifestForDistributedScan(
    ClientContext &context, const string &path, uint64_t version,
    const string &serialized_manifest, const string &expected_generation);
#endif

string LanceNormalizeS3Scheme(const string &path);
#ifdef LANCE_VANE_DISTRIBUTED
// Canonicalize a syntactically valid URI scheme for secret matching. URI
// schemes are ASCII case-insensitive; Lance also treats s3a/s3n as s3 aliases.
string LanceVaneCanonicalizeSecretScope(const string &scope);
#endif
void LanceFillStorageOptionsFromSecrets(ClientContext &context,
                                        const string &path,
                                        vector<string> &out_keys,
                                        vector<string> &out_values);
void ResolveLanceStorageOptions(ClientContext &context, const string &path,
                                string &out_open_path,
                                vector<string> &out_option_keys,
                                vector<string> &out_option_values);
#ifdef LANCE_VANE_DISTRIBUTED
void ResolveLanceStorageOptionsForDistributedRead(
    ClientContext &context, const string &path, string &out_open_path,
    vector<string> &out_option_keys, vector<string> &out_option_values);
bool LanceHasMatchingStorageSecret(ClientContext &context, const string &path);
// Return whether a URI carries components that must never appear in Vane plan
// snapshots or diagnostics. Plain filesystem paths are not parsed as URIs.
bool LanceVanePathHasPrivateUriComponents(const string &path);
// Return whether Lance resolves the path through a remote object-store or
// service URI. Remote backend diagnostics are always treated as opaque.
bool LanceVanePathIsRemote(const string &path);
// Include remote-backend and resolved storage-option provenance in the
// diagnostic taint. This must be evaluated before the first backend call
// because an error response may repeat ambient/request credentials, vended
// URLs, or private endpoint values.
bool LanceVanePathRequiresRedaction(ClientContext &context, const string &path);
// Return whether Lance's normalized URL/path semantics identify the input as
// a dataset path. Replacement scans use this instead of reparsing URI text in
// C++ so credential-bearing normalized forms cannot fall through to DuckDB.
bool LanceVanePathIsLanceDataset(const string &path);
// Redact a URI with private components, or a path whose provenance requires
// redaction, before exposing it through errors or EXPLAIN output.
string LanceVaneDiagnosticPath(const string &path,
                               bool force_redaction = false);
// Consume the current Lance FFI error while suppressing its details whenever
// they could contain a private URI.
string LanceVaneFormatErrorSuffix(const string &path,
                                  bool force_redaction = false);
// Return a canonical worker-replay path, or an empty string when the path is
// process-local or could expose URI credentials through a serialized plan.
string LanceVaneReplayPath(ClientContext &context, const string &path);
#endif
void BuildStorageOptionPointerArrays(const vector<string> &option_keys,
                                     const vector<string> &option_values,
                                     vector<const char *> &out_key_ptrs,
                                     vector<const char *> &out_value_ptrs);

static constexpr uint64_t LANCE_DEFAULT_MAX_ROWS_PER_FILE = 1024ULL * 1024ULL;
static constexpr uint64_t LANCE_DEFAULT_MAX_ROWS_PER_GROUP = 1024ULL;
static constexpr uint64_t LANCE_DEFAULT_MAX_BYTES_PER_FILE =
    90ULL * 1024ULL * 1024ULL * 1024ULL;
static constexpr const char *LANCE_DEFAULT_DATA_STORAGE_VERSION = "2.2";

void ResolveLanceNamespaceAuth(ClientContext &context, const string &endpoint,
                               const unordered_map<string, Value> &options,
                               string &out_bearer_token, string &out_api_key);
void ResolveLanceNamespaceAuth(ClientContext &context, const string &endpoint,
                               const named_parameter_map_t &options,
                               string &out_bearer_token, string &out_api_key);
void ResolveLanceNamespaceAuthOverrides(
    const unordered_map<string, Value> &options, string &out_bearer_token,
    string &out_api_key);

bool TryLanceNamespaceListTables(ClientContext &context, const string &endpoint,
                                 const string &namespace_id,
                                 const string &bearer_token,
                                 const string &api_key, const string &delimiter,
                                 const string &headers_tsv,
                                 vector<string> &out_tables, string &out_error);

bool TryLanceDirNamespaceListTables(ClientContext &context, const string &root,
                                    vector<string> &out_tables,
                                    string &out_error);

void *
LanceOpenDatasetInNamespace(ClientContext &context, const string &endpoint,
                            const string &table_id, const string &bearer_token,
                            const string &api_key, const string &delimiter,
                            const string &headers_tsv, string &out_table_uri);

bool TryLanceNamespaceDescribeTable(
    ClientContext &context, const string &endpoint, const string &table_id,
    const string &bearer_token, const string &api_key, const string &delimiter,
    const string &headers_tsv, string &out_location,
    vector<string> &out_option_keys, vector<string> &out_option_values,
    string &out_error);

bool TryLanceNamespaceCreateEmptyTable(
    ClientContext &context, const string &endpoint, const string &table_id,
    const string &bearer_token, const string &api_key, const string &delimiter,
    const string &headers_tsv, string &out_location,
    vector<string> &out_option_keys, vector<string> &out_option_values,
    string &out_error);

bool TryLanceNamespaceDropTable(ClientContext &context, const string &endpoint,
                                const string &table_id,
                                const string &bearer_token,
                                const string &api_key, const string &delimiter,
                                const string &headers_tsv, string &out_error);

struct LanceNamespaceTableConfig;
class LanceTableEntry;

#ifdef LANCE_VANE_DISTRIBUTED
// Return whether diagnostics for this table must suppress the supplied path
// and the current Lance FFI error. Namespace resolution can normalize a
// private ATTACH URI into a path that no longer carries the original marker,
// so this check also consults the catalog-entry provenance bits.
bool LanceVaneTablePathRequiresRedaction(const LanceTableEntry &table,
                                         const string &path);
string LanceVaneTableDiagnosticPath(const LanceTableEntry &table,
                                    const string &path);
string LanceVaneTableFormatErrorSuffix(const LanceTableEntry &table,
                                       const string &path);
// Return whether the current query-session storage settings exactly match
// those captured when a directory namespace was attached.
bool LanceVaneDirectoryNamespaceSessionMatches(
    ClientContext &context, const LanceNamespaceTableConfig &config);
#endif

void FillLanceNamespaceQueryConfig(
    ClientContext &context, const LanceNamespaceTableConfig &cfg, uint64_t k,
    bool prefilter, const string &filter, const vector<string> &columns,
    vector<const char *> &option_key_ptrs,
    vector<const char *> &option_value_ptrs, vector<const char *> &column_ptrs,
    string &bearer_token, string &api_key,
    ::LanceNamespaceQueryConfig &out_config);

string LanceDirectoryNamespaceDatasetUri(const LanceNamespaceTableConfig &cfg);

// Resolve a string like "catalog.schema.table" to a LanceTableEntry* if it
// names a Lance-backed table in an attached catalog.  Returns nullptr for
// inputs that look like filesystem / URL paths, or when the lookup does not
// yield a Lance table (missing entry, non-Lance table, malformed qualified
// name).  This is the canonical way to re-resolve a dataset from its
// original first-argument literal so we can take the namespace-aware
// LanceOpenDatasetForTable() path instead of passing the virtual
// namespace URI directly to lance-io.
LanceTableEntry *TryResolveLanceTableEntry(ClientContext &context,
                                           const string &input);

void *LanceOpenDatasetForTable(ClientContext &context,
                               const LanceTableEntry &table,
                               string &out_display_uri);

void ResolveLanceStorageOptionsForTable(ClientContext &context,
                                        const LanceTableEntry &table,
                                        string &out_open_path,
                                        vector<string> &out_option_keys,
                                        vector<string> &out_option_values,
                                        string &out_display_uri);

int64_t LanceTruncateDatasetWithStorageOptions(
    ClientContext &context, const string &open_path,
    const vector<string> &option_keys, const vector<string> &option_values,
    const string &display_uri);

int64_t LanceTruncateDataset(ClientContext &context, const string &dataset_uri);

} // namespace duckdb
