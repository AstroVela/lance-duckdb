#ifdef LANCE_VANE_DISTRIBUTED

#include "lance_vane_rest_resolution.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"

#include "lance_arrow_compat.hpp"
#include "lance_common.hpp"
#include "lance_ffi.hpp"
#include "lance_table_entry.hpp"

#include <cstring>

namespace duckdb {

static constexpr idx_t LANCE_VANE_SCHEMA_FINGERPRINT_SIZE = 32;

struct LanceVaneDatasetCloser {
  void operator()(void *dataset) const {
    if (dataset) {
      lance_close_dataset(dataset);
    }
  }
};

struct LanceVaneCStringCloser {
  void operator()(const char *value) const {
    if (value) {
      lance_free_string(value);
    }
  }
};

static void SetRestCandidateFailure(LanceVanePhysicalCandidate &candidate,
                                    const string &message) {
  candidate = {};
  candidate.attempted = true;
  candidate.source_class = LanceVaneSearchSourceClass::STANDARD_REST;
  candidate.private_uri_diagnostics = true;
  candidate.safe_failure = message;
}

static bool PopulateArrowSchemaForComparison(ClientContext &context,
                                             ArrowSchema &schema,
                                             vector<string> &out_names,
                                             vector<LogicalType> &out_types) {
  try {
    LanceCoerceArrowSchemaForDuckDB(&schema);
    ArrowTableSchema table_schema;
    ArrowTableFunction::PopulateArrowTableSchema(context, table_schema, schema);
    out_names = table_schema.GetNames();
    out_types = table_schema.GetTypes();
    return !out_names.empty() && out_names.size() == out_types.size();
  } catch (Exception &) {
    return false;
  }
}

static bool FingerprintArrowSchema(ArrowSchema &schema,
                                   string &out_fingerprint) {
  out_fingerprint.assign(LANCE_VANE_SCHEMA_FINGERPRINT_SIZE, '\0');
  if (lance_vane_arrow_schema_fingerprint(
          &schema, reinterpret_cast<uint8_t *>(&out_fingerprint[0])) != 0) {
    LanceConsumeLastError();
    out_fingerprint.clear();
    return false;
  }
  return true;
}

static bool ReadRestSchema(ClientContext &context, const string &schema_json,
                           vector<string> &out_names,
                           vector<LogicalType> &out_types,
                           string &out_fingerprint) {
  ArrowSchemaWrapper schema;
  std::memset(&schema.arrow_schema, 0, sizeof(schema.arrow_schema));
  if (lance_json_arrow_schema_to_c(schema_json.c_str(), &schema.arrow_schema) !=
      0) {
    LanceConsumeLastError();
    return false;
  }
  return FingerprintArrowSchema(schema.arrow_schema, out_fingerprint) &&
         PopulateArrowSchemaForComparison(context, schema.arrow_schema,
                                          out_names, out_types);
}

static bool ReadDatasetSchema(ClientContext &context, void *dataset,
                              vector<string> &out_names,
                              vector<LogicalType> &out_types,
                              string &out_fingerprint) {
  auto *schema_handle = lance_get_schema(dataset);
  if (!schema_handle) {
    LanceConsumeLastError();
    return false;
  }
  ArrowSchemaWrapper schema;
  std::memset(&schema.arrow_schema, 0, sizeof(schema.arrow_schema));
  auto rc = lance_schema_to_arrow(schema_handle, &schema.arrow_schema);
  lance_free_schema(schema_handle);
  if (rc != 0) {
    LanceConsumeLastError();
    return false;
  }
  return FingerprintArrowSchema(schema.arrow_schema, out_fingerprint) &&
         PopulateArrowSchemaForComparison(context, schema.arrow_schema,
                                          out_names, out_types);
}

static bool ReadDatasetGeneration(void *dataset, string &out_generation) {
  auto *generation_ptr = lance_dataset_generation_id(dataset);
  if (!generation_ptr) {
    LanceConsumeLastError();
    return false;
  }
  out_generation = generation_ptr;
  lance_free_string(generation_ptr);
  return !out_generation.empty() && out_generation.find('\0') == string::npos;
}

static bool MatchesCatalogSchema(const LanceTableEntry &table,
                                 const vector<string> &names,
                                 const vector<LogicalType> &types) {
  auto columns = table.GetColumns().Physical();
  if (columns.Size() != names.size() || names.size() != types.size()) {
    return false;
  }
  idx_t index = 0;
  for (auto &column : columns) {
    if (column.Name() != names[index] || column.Type() != types[index]) {
      return false;
    }
    index++;
  }
  return true;
}

void LanceVaneResolveRestPhysicalCandidate(
    ClientContext &context, const LanceTableEntry &table,
    const shared_ptr<LanceDatasetCacheEntry> &bound_dataset_entry,
    LanceVanePhysicalCandidate &out_candidate) {
  SetRestCandidateFailure(
      out_candidate,
      "Distributed Lance REST reads could not resolve a physical table");
  if (!table.IsNamespaceBacked() || !table.NamespaceConfig().IsRest()) {
    out_candidate.safe_failure =
        "Distributed Lance REST resolution requires a REST namespace table";
    return;
  }

  if (!bound_dataset_entry || !bound_dataset_entry->Handle()) {
    out_candidate.safe_failure =
        "Distributed Lance REST reads require the bound namespace snapshot";
    return;
  }

  auto *bound_dataset = bound_dataset_entry->Handle();
  auto bound_version = lance_dataset_version(bound_dataset);
  if (bound_version == 0 || bound_version > NumericLimits<int64_t>::Maximum()) {
    out_candidate.safe_failure =
        "Distributed Lance REST reads could not identify the bound namespace "
        "version";
    return;
  }

  string bound_generation;
  vector<string> bound_names;
  vector<LogicalType> bound_types;
  string bound_fingerprint;
  if (!ReadDatasetGeneration(bound_dataset, bound_generation) ||
      !ReadDatasetSchema(context, bound_dataset, bound_names, bound_types,
                         bound_fingerprint) ||
      !MatchesCatalogSchema(table, bound_names, bound_types)) {
    out_candidate.safe_failure =
        "Distributed Lance REST reads could not validate the bound namespace "
        "snapshot";
    return;
  }

  auto &cfg = table.NamespaceConfig();
  vector<const char *> option_key_ptrs;
  vector<const char *> option_value_ptrs;
  vector<const char *> column_ptrs;
  string bearer_token;
  string api_key;
  LanceNamespaceQueryConfig query_config;
  try {
    FillLanceNamespaceQueryConfig(
        context, cfg, 0, false, string(), vector<string>(), option_key_ptrs,
        option_value_ptrs, column_ptrs, bearer_token, api_key, query_config);
  } catch (Exception &) {
    out_candidate.safe_failure =
        "Distributed Lance REST reads could not resolve coordinator control-"
        "plane authentication";
    return;
  }

  const char *table_uri_ptr = nullptr;
  const char *schema_json_ptr = nullptr;
  uint64_t described_version = 0;
  auto rc = lance_vane_resolve_rest_table(
      cfg.endpoint.c_str(), cfg.table_id.c_str(),
      bearer_token.empty() ? nullptr : bearer_token.c_str(),
      api_key.empty() ? nullptr : api_key.c_str(),
      cfg.delimiter.empty() ? nullptr : cfg.delimiter.c_str(),
      cfg.headers_tsv.empty() ? nullptr : cfg.headers_tsv.c_str(),
      bound_version, &table_uri_ptr, &schema_json_ptr, &described_version);
  unique_ptr<const char, LanceVaneCStringCloser> table_uri_owner(table_uri_ptr);
  unique_ptr<const char, LanceVaneCStringCloser> schema_json_owner(
      schema_json_ptr);
  if (rc != 0 || !table_uri_ptr || !schema_json_ptr || described_version == 0) {
    // The control-plane response may contain credentials or opaque service
    // details. Consume it, but never copy it into a plan or user-facing error.
    LanceConsumeLastError();
    out_candidate.safe_failure =
        "Distributed Lance REST reads require a standard DescribeTable "
        "response with table_uri, schema, and version";
    return;
  }
  if (described_version != bound_version) {
    out_candidate.safe_failure =
        "Distributed Lance REST reads require DescribeTable to resolve the "
        "bound namespace version";
    return;
  }

  string table_uri(table_uri_ptr);
  string schema_json(schema_json_ptr);
  auto replay_path = LanceVaneReplayPath(context, table_uri);
  if (replay_path.empty()) {
    out_candidate.safe_failure =
        "Distributed Lance REST reads require a replayable physical table_uri";
    return;
  }
  if (LanceHasMatchingStorageSecret(context, replay_path)) {
    out_candidate.safe_failure =
        "Distributed Lance REST reads cannot depend on a coordinator-only "
        "storage secret";
    return;
  }

  std::unique_ptr<void, LanceVaneDatasetCloser> latest(
      LanceOpenDatasetForDistributedScan(context, replay_path));
  if (!latest) {
    LanceConsumeLastError();
    out_candidate.safe_failure =
        "Distributed Lance REST reads could not open the resolved physical "
        "table_uri with the query-session data-plane configuration";
    return;
  }

  std::unique_ptr<void, LanceVaneDatasetCloser> fixed;
  void *dataset = latest.get();
  if (lance_dataset_version(dataset) != described_version) {
    fixed.reset(lance_dataset_checkout_version(dataset, described_version));
    if (!fixed) {
      LanceConsumeLastError();
      out_candidate.safe_failure =
          "Distributed Lance REST reads could not open the exact described "
          "table version";
      return;
    }
    dataset = fixed.get();
  }
  if (lance_dataset_version(dataset) != described_version) {
    out_candidate.safe_failure =
        "Distributed Lance REST reads observed a version mismatch between "
        "DescribeTable and the physical dataset";
    return;
  }

  vector<string> described_names;
  vector<LogicalType> described_types;
  vector<string> physical_names;
  vector<LogicalType> physical_types;
  string described_fingerprint;
  string physical_fingerprint;
  string physical_generation;
  if (!ReadRestSchema(context, schema_json, described_names, described_types,
                      described_fingerprint) ||
      !ReadDatasetSchema(context, dataset, physical_names, physical_types,
                         physical_fingerprint) ||
      !ReadDatasetGeneration(dataset, physical_generation) ||
      described_fingerprint != physical_fingerprint ||
      described_fingerprint != bound_fingerprint ||
      described_names != physical_names || described_types != physical_types ||
      described_names != bound_names || described_types != bound_types ||
      physical_generation != bound_generation ||
      !MatchesCatalogSchema(table, physical_names, physical_types)) {
    out_candidate.safe_failure =
        "Distributed Lance REST reads require matching bound, DescribeTable, "
        "and physical snapshots";
    return;
  }

  if (fixed) {
    latest.reset();
    dataset = fixed.release();
  } else {
    dataset = latest.release();
  }
  auto entry =
      make_shared_ptr<LanceDatasetCacheEntry>(dataset, std::move(replay_path));
  LanceVaneCapturePhysicalCandidate(context, entry->DisplayUri(), entry,
                                    LanceVaneSearchSourceClass::STANDARD_REST,
                                    true, false, out_candidate);
  if (!out_candidate.qualified ||
      out_candidate.dataset_version != bound_version ||
      out_candidate.dataset_generation_id != bound_generation) {
    SetRestCandidateFailure(
        out_candidate,
        "Distributed Lance REST reads could not freeze the bound namespace "
        "snapshot");
  }
}

} // namespace duckdb

#endif
