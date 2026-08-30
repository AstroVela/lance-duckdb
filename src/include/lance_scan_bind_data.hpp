#pragma once

#include "lance_dataset_cache.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#ifdef LANCE_VANE_DISTRIBUTED
#include "lance_vane_search.hpp"
#endif

#include <cstdint>

namespace duckdb {
class TableCatalogEntry;
struct LanceNamespaceTableConfig;

struct LanceScanBindData : public TableFunctionData {
  string file_path;
  bool explain_verbose = false;
  shared_ptr<LanceDatasetCacheEntry> dataset_entry;
  void *dataset = nullptr;
  bool dataset_cache_hit = false;
  ArrowSchemaWrapper schema_root;
  ArrowSchemaWrapper scan_schema_root;
  ArrowTableSchema arrow_table;
  ArrowTableSchema scan_arrow_table;
  vector<string> names;
  vector<LogicalType> types;
  vector<string> lance_pushed_filter_ir_parts;
  vector<string> duckdb_pushed_filter_sql_parts;
  optional_ptr<TableCatalogEntry> table_entry = nullptr;
  unique_ptr<LanceNamespaceTableConfig> namespace_query_config;

#ifdef LANCE_VANE_DISTRIBUTED
  uint64_t dataset_version = 0;
  string dataset_generation_id;
  string distributed_scan_token;
  bool distributed_replayable = false;
  bool distributed_replay_path_restricted = false;
  bool distributed_requires_coordinator_secret = false;
  // Suppress backend error details whenever a URI, DuckDB secret, or opaque
  // namespace response may carry credentials. This is diagnostic provenance;
  // it is intentionally independent of worker replay eligibility.
  bool private_uri_diagnostics = false;
  bool distributed_namespace_session_mismatch = false;
  bool distributed_worker = false;
  bool distributed_splits_applied = false;
  bool distributed_authorization_restricted = false;
  vector<string> distributed_authorized_split_ids;
  vector<string> distributed_authorized_split_payloads;
  // Portable coordinator-planning metadata. Vane may translate a serialized
  // worker plan again before assigning splits, when no process-local dataset
  // handle is available.
  vector<uint64_t> distributed_fragment_ids;
  vector<int64_t> distributed_fragment_row_counts;
  vector<uint64_t> distributed_fragment_bytes_on_disk;
  vector<uint64_t> selected_fragment_ids;
  // Coordinator-only standard REST resolution result. It is never serialized;
  // an admitted worker bind is converted to the existing direct physical scan
  // representation before transport.
  LanceVanePhysicalCandidate distributed_rest_candidate;
#endif

  bool sampling_pushed_down = false;
  double sample_percentage = 0.0;
  int64_t sample_seed = -1;
  bool sample_repeatable = false;
  vector<uint64_t> take_row_ids;

  bool limit_offset_pushed_down = false;
  optional_idx pushed_limit = optional_idx::Invalid();
  idx_t pushed_offset = 0;

  bool UsesNamespaceQuery() const { return namespace_query_config != nullptr; }

#ifdef LANCE_VANE_DISTRIBUTED
  unique_ptr<FunctionData> Copy() const override;
#endif
  ~LanceScanBindData() override;
};

} // namespace duckdb
