#pragma once

#ifdef LANCE_VANE_DISTRIBUTED

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/distributed_table_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"

#include "lance_dataset_cache.hpp"
#include "lance_vane_snapshot.hpp"

namespace duckdb {

static constexpr idx_t LANCE_VANE_MAX_SERIALIZED_INDEX_SECTION_BYTES =
    256ULL * 1024ULL * 1024ULL;

struct LanceVaneFrozenSearchSnapshot {
  LanceVaneFrozenSnapshot dataset;
  string serialized_index_section;
  string index_section_sha256;
};

enum class LanceVaneSearchKind : uint8_t { VECTOR = 0, FTS = 1, HYBRID = 2 };
enum class LanceVaneSearchTaskVariant : uint8_t {
  FINAL_SEARCH = 0,
  VECTOR_CANDIDATES = 1,
  FTS_CANDIDATES = 2
};
enum class LanceVaneSearchOverload : uint8_t {
  VECTOR_FLOAT = 0,
  VECTOR_DOUBLE = 1,
  FTS = 2,
  HYBRID_FLOAT = 3,
  HYBRID_DOUBLE = 4
};
enum class LanceVaneSearchSourceClass : uint8_t {
  DIRECT = 0,
  DIRECTORY_NAMESPACE = 1,
  STANDARD_REST = 2
};

struct LanceVanePhysicalCandidate {
  bool attempted = false;
  bool qualified = false;
  string safe_failure;
  LanceVaneSearchSourceClass source_class = LanceVaneSearchSourceClass::DIRECT;
  string physical_uri;
  uint64_t dataset_version = 0;
  string dataset_generation_id;
  string schema_fingerprint;
  string search_node_uuid;
  bool private_uri_diagnostics = false;
  shared_ptr<LanceDatasetCacheEntry> dataset_entry;
  void *dataset = nullptr;
  ClientContext *context = nullptr;
};

struct LanceVaneSearchArguments {
  LanceVaneSearchKind kind = LanceVaneSearchKind::VECTOR;
  LanceVaneSearchOverload overload = LanceVaneSearchOverload::VECTOR_FLOAT;
  string vector_column;
  vector<float> vector_query;
  string text_column;
  string text_query;
  uint64_t k = 0;
  uint64_t nprobes = 0;
  uint64_t refine_factor = 0;
  bool prefilter = false;
  bool use_index = true;
  bool explain_verbose = false;
  bool namespace_backed = false;
  float alpha = 0.5F;
  uint32_t oversample_factor = 4;
  string namespace_filter;
};

struct LanceVaneGlobalSearchState {
  bool valid = false;
  bool finalized = false;
  uint64_t contract_version = 1;
  string qualification_failure;
  LanceVaneSearchSourceClass source_class = LanceVaneSearchSourceClass::DIRECT;
  string physical_uri;
  uint64_t dataset_version = 0;
  string dataset_generation_id;
  string schema_fingerprint;
  string search_node_uuid;
  bool private_uri_diagnostics = false;

  LanceVaneSearchArguments arguments;
  vector<string> output_names;
  vector<LogicalType> output_types;
  vector<ColumnIndex> column_ids;
  vector<idx_t> projection_ids;
  string final_filter_ir;
  string namespace_filter_plan;
  string filter_fingerprint;
  bool filter_pushed_down = false;
  vector<string> pending_filter_ir_parts;
  bool pending_complex_filter_pushdown_failed = false;
  string index_plan;
  bool search_plan_payloads_validated = false;
  shared_ptr<const LanceVaneFrozenSearchSnapshot> frozen_snapshot;
  bool frozen_snapshot_payload_validated = false;
  LanceVaneSearchTaskVariant execution_variant =
      LanceVaneSearchTaskVariant::FINAL_SEARCH;
  vector<uint64_t> fragment_ids;
  vector<int64_t> fragment_row_counts;
  vector<uint64_t> fragment_bytes_on_disk;
  string state_sha256;

  bool worker_bind = false;
  bool task_assignment_applied = false;
  bool empty_assignment = false;
  bool authorization_restricted = false;
  vector<string> authorized_task_ids;
  vector<string> authorized_task_payloads;
  vector<uint64_t> selected_fragment_ids;
};

void LanceVaneCapturePhysicalCandidate(
    ClientContext &context, const string &physical_uri,
    const shared_ptr<LanceDatasetCacheEntry> &dataset_entry,
    LanceVaneSearchSourceClass source_class, bool private_uri_diagnostics,
    bool requires_coordinator_storage_secret,
    LanceVanePhysicalCandidate &out_candidate);

LanceVaneGlobalSearchState
LanceVanePrepareGlobalSearchState(const LanceVanePhysicalCandidate &candidate,
                                  const LanceVaneSearchArguments &arguments,
                                  const vector<string> &output_names,
                                  const vector<LogicalType> &output_types);

void LanceVaneAccumulatePendingGlobalSearchFilters(
    LanceVaneGlobalSearchState &state,
    const vector<string> &pushed_filter_ir_parts,
    bool complex_filter_pushdown_failed);

LanceVaneGlobalSearchState LanceVaneFinalizeGlobalSearchState(
    const TableFunctionDistributedScanInput &input,
    const LanceVaneGlobalSearchState &prepared_state,
    const vector<string> &pushed_filter_ir_parts,
    bool complex_filter_pushdown_failed);

bool LanceVaneTryEnableExactVectorCandidates(LanceVaneGlobalSearchState &state,
                                             bool has_postfilter);

vector<DistributedScanSplit>
LanceVaneCreateSearchTaskAssignments(const LanceVaneGlobalSearchState &state);

void LanceVanePrepareSearchWorkerBindState(LanceVaneGlobalSearchState &state);

void LanceVaneApplySearchTaskAssignments(
    LanceVaneGlobalSearchState &state,
    const vector<DistributedScanSplit> &splits);

void LanceVaneSerializeGlobalSearchState(
    Serializer &serializer, const LanceVaneGlobalSearchState &state);
LanceVaneGlobalSearchState
LanceVaneDeserializeGlobalSearchState(Deserializer &deserializer);

shared_ptr<LanceDatasetCacheEntry>
LanceVaneOpenSearchSnapshot(ClientContext &context,
                            const LanceVaneGlobalSearchState &state);

shared_ptr<LanceDatasetCacheEntry>
LanceVaneOpenSearchSnapshotForMaterialization(
    ClientContext &context, const LanceVaneGlobalSearchState &state);

void LanceVanePopulateSearchSchema(ClientContext &context,
                                   const vector<string> &names,
                                   const vector<LogicalType> &types,
                                   ArrowSchemaWrapper &schema_root,
                                   ArrowTableSchema &arrow_table);

void LanceVaneValidateExecutionInput(const TableFunctionInitInput &input,
                                     const LanceVaneGlobalSearchState &state);

void LanceVaneValidateDistributedInput(
    const TableFunctionDistributedScanInput &input,
    const LanceVaneGlobalSearchState &state);

TableFunctionDistributedScanCallbacks LanceVaneSearchTaskCallbacks(
    table_function_plan_distributed_scan_splits_t plan_splits,
    table_function_create_distributed_worker_bind_t create_worker_bind,
    table_function_apply_distributed_scan_splits_t apply_splits);

} // namespace duckdb

#endif
