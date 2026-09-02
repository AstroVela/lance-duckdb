#pragma once

#include "duckdb/common/arrow/arrow.hpp"

#include <cstddef>
#include <cstdint>

extern "C" {
typedef struct LanceSessionStats {
  uint64_t size_bytes;
  uint64_t approx_num_items;
} LanceSessionStats;

typedef struct LanceDebugCounters {
  uint64_t dataset_open_count;
  uint64_t namespace_describe_count;
  uint64_t commit_count;
} LanceDebugCounters;

void *lance_create_session(uint64_t index_cache_size_bytes,
                           uint64_t metadata_cache_size_bytes);
void lance_close_session(void *session);
int32_t lance_session_get_stats(void *session, LanceSessionStats *out_stats);
int32_t lance_debug_get_counters(LanceDebugCounters *out_counters);
void lance_debug_reset_counters();

#ifdef LANCE_VANE_DISTRIBUTED
typedef struct LanceVaneSessionCacheStats {
  uint64_t index_hits;
  uint64_t index_misses;
  uint64_t index_num_entries;
  uint64_t index_size_bytes;
  uint64_t metadata_hits;
  uint64_t metadata_misses;
  uint64_t metadata_num_entries;
  uint64_t metadata_size_bytes;
} LanceVaneSessionCacheStats;

uint64_t lance_vane_default_index_cache_size_bytes();
uint64_t lance_vane_default_metadata_cache_size_bytes();
int32_t
lance_vane_session_get_cache_stats(void *session,
                                   LanceVaneSessionCacheStats *out_stats);

// Bit flags returned by lance_vane_classify_path. The implementation uses the
// same URL conversion routine as Lance's object-store layer.
static constexpr uint8_t LANCE_VANE_PATH_IS_URI = 1U << 0;
static constexpr uint8_t LANCE_VANE_PATH_HAS_PRIVATE_COMPONENTS = 1U << 1;
static constexpr uint8_t LANCE_VANE_PATH_IS_PROCESS_LOCAL = 1U << 2;
static constexpr uint8_t LANCE_VANE_PATH_INVALID = 1U << 3;
static constexpr uint8_t LANCE_VANE_PATH_IS_LANCE_DATASET = 1U << 4;
static constexpr uint8_t LANCE_VANE_PATH_IS_REMOTE = 1U << 5;
uint8_t lance_vane_classify_path(const uint8_t *path, size_t path_len);
#endif

void *lance_open_dataset(const char *path);
void *lance_open_dataset_with_session(const char *path, void *session);
void *lance_open_dataset_with_storage_options(const char *path,
                                              const char **option_keys,
                                              const char **option_values,
                                              size_t options_len);
void *lance_open_dataset_with_storage_options_and_session(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, void *session);
const char *lance_dir_namespace_list_tables(const char *root,
                                            const char **option_keys,
                                            const char **option_values,
                                            size_t options_len);
int32_t lance_dir_namespace_drop_table(const char *root, const char *table_name,
                                       const char **option_keys,
                                       const char **option_values,
                                       size_t options_len);
void *lance_open_dataset_in_dir_namespace(
    const char *root, const char *table_name, const char **option_keys,
    const char **option_values, size_t options_len, const char **out_table_uri);
void *lance_open_dataset_in_dir_namespace_with_session(
    const char *root, const char *table_name, const char **option_keys,
    const char **option_values, size_t options_len, void *session,
    const char **out_table_uri);
const char *
lance_namespace_list_tables(const char *endpoint, const char *namespace_id,
                            const char *bearer_token, const char *api_key,
                            const char *delimiter, const char *headers_tsv);
int32_t lance_json_arrow_schema_to_c(const char *json_schema,
                                     ArrowSchema *out_schema);
int32_t lance_namespace_describe_table_with_schema(
    const char *endpoint, const char *table_id, const char *bearer_token,
    const char *api_key, const char *delimiter, const char *headers_tsv,
    const char **out_schema_json);
int32_t lance_namespace_describe_table(
    const char *endpoint, const char *table_id, const char *bearer_token,
    const char *api_key, const char *delimiter, const char *headers_tsv,
    const char **out_location, const char **out_storage_options_tsv);
int32_t lance_namespace_create_empty_table(
    const char *endpoint, const char *table_id, const char *bearer_token,
    const char *api_key, const char *delimiter, const char *headers_tsv,
    const char **out_location, const char **out_storage_options_tsv);
int32_t lance_namespace_drop_table(const char *endpoint, const char *table_id,
                                   const char *bearer_token,
                                   const char *api_key, const char *delimiter,
                                   const char *headers_tsv);
void *
lance_open_dataset_in_namespace(const char *endpoint, const char *table_id,
                                const char *bearer_token, const char *api_key,
                                const char *delimiter, const char *headers_tsv,
                                const char **out_table_uri);
void *lance_open_dataset_in_namespace_with_session(
    const char *endpoint, const char *table_id, const char *bearer_token,
    const char *api_key, const char *delimiter, const char *headers_tsv,
    void *session, const char **out_table_uri);
void lance_close_dataset(void *dataset);
#ifdef LANCE_VANE_DISTRIBUTED
uint64_t lance_dataset_version(void *dataset);
const char *lance_dataset_generation_id(void *dataset);
void *lance_dataset_checkout_version(void *dataset, uint64_t version);
void *lance_vane_open_dataset_version_with_session(const char *path,
                                                   uint64_t version,
                                                   void *session);
void *lance_vane_open_dataset_version_with_storage_options_and_session(
    const char *path, uint64_t version, const char **option_keys,
    const char **option_values, size_t options_len, void *session);
int32_t lance_vane_serialize_dataset_manifest(void *dataset, uint8_t **out_data,
                                              size_t *out_len);
void *lance_vane_open_dataset_version_from_manifest_with_session(
    const char *path, uint64_t version, const uint8_t *manifest,
    size_t manifest_len, const char *expected_generation, void *session);
void *
lance_vane_open_dataset_version_from_manifest_with_storage_options_and_session(
    const char *path, uint64_t version, const uint8_t *manifest,
    size_t manifest_len, const char *expected_generation,
    const char **option_keys, const char **option_values, size_t options_len,
    void *session);
void *
lance_vane_open_dataset_version_from_manifest_and_index_section_with_session(
    const char *path, uint64_t version, const uint8_t *manifest,
    size_t manifest_len, const uint8_t *index_section, size_t index_section_len,
    const char *expected_generation, void *session);
void *
lance_vane_open_dataset_version_from_manifest_and_index_section_with_storage_options_and_session(
    const char *path, uint64_t version, const uint8_t *manifest,
    size_t manifest_len, const uint8_t *index_section, size_t index_section_len,
    const char *expected_generation, const char **option_keys,
    const char **option_values, size_t options_len, void *session);

int32_t lance_vane_sha256(const uint8_t *input, size_t input_len,
                          uint8_t *output);
void lance_vane_free_bytes(uint8_t *data, size_t len);
int32_t lance_vane_dataset_schema_fingerprint(void *dataset, uint8_t *output);
int32_t lance_vane_arrow_schema_fingerprint(const ArrowSchema *schema,
                                            uint8_t *output);
int32_t lance_vane_plan_namespace_filter(void *dataset, const char *sql,
                                         uint8_t **out_data, size_t *out_len);
int32_t lance_vane_serialize_dataset_index_section(void *dataset,
                                                   uint8_t **out_data,
                                                   size_t *out_len);
int32_t lance_vane_build_search_index_plan(
    void *dataset, const char *generation, uint8_t search_kind,
    const char *vector_column, const char *text_column,
    uint8_t use_vector_index, uint8_t **out_data, size_t *out_len);
int32_t lance_vane_validate_search_index_plan(
    const uint8_t *data, size_t len, uint64_t dataset_version,
    const char *generation, uint8_t search_kind, const char *vector_column,
    const char *text_column, uint8_t use_vector_index);
int32_t lance_vane_validate_namespace_filter_plan(const uint8_t *data,
                                                  size_t len);
void *lance_vane_create_knn_stream_ir(
    void *dataset, const char *generation, const char *vector_column,
    const float *query_values, size_t query_len, uint64_t k, uint64_t nprobes,
    uint64_t refine_factor, const uint8_t *filter_ir, size_t filter_ir_len,
    const uint8_t *namespace_filter_plan, size_t namespace_filter_plan_len,
    uint8_t prefilter, uint8_t use_index, const uint8_t *index_plan,
    size_t index_plan_len);
void *lance_vane_create_fts_stream_ir(
    void *dataset, const char *generation, const char *text_column,
    const char *query, uint64_t k, const uint8_t *filter_ir,
    size_t filter_ir_len, const uint8_t *namespace_filter_plan,
    size_t namespace_filter_plan_len, uint8_t prefilter,
    const uint8_t *index_plan, size_t index_plan_len);
void *lance_vane_create_hybrid_stream_ir(
    void *dataset, const char *generation, const char *vector_column,
    const float *query_values, size_t query_len, const char *text_column,
    const char *text_query, uint64_t k, uint64_t nprobes,
    uint64_t refine_factor, const uint8_t *filter_ir, size_t filter_ir_len,
    const uint8_t *namespace_filter_plan, size_t namespace_filter_plan_len,
    uint8_t prefilter, uint8_t use_index, float alpha,
    uint32_t oversample_factor, const uint8_t *index_plan,
    size_t index_plan_len);
int32_t lance_vane_resolve_rest_table(
    const char *endpoint, const char *table_id, const char *bearer_token,
    const char *api_key, const char *delimiter, const char *headers_tsv,
    uint64_t expected_version, const char **out_table_uri,
    const char **out_schema_json, uint64_t *out_version);
#endif

void *lance_get_schema(void *dataset);
void *lance_get_schema_for_scan(void *dataset);
void lance_free_schema(void *schema);
int32_t lance_schema_to_arrow(void *schema, ArrowSchema *out_schema);

int32_t lance_stream_next(void *stream, void **out_batch);
void lance_close_stream(void *stream);

void *lance_get_exec_schema(void *dataset, const uint8_t *exec_ir,
                            size_t exec_ir_len);
void *lance_create_dataset_exec_stream_ir(void *dataset, const uint8_t *exec_ir,
                                          size_t exec_ir_len);

int32_t lance_last_error_code();
const char *lance_last_error_message();
void lance_free_string(const char *s);

int64_t lance_dataset_count_rows(void *dataset);
int32_t lance_dataset_delete(void *dataset, const uint8_t *filter_ir,
                             size_t filter_ir_len, int64_t *out_deleted_rows);
int32_t lance_delete_transaction_with_storage_options(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, const uint8_t *filter_ir, size_t filter_ir_len,
    void *session, void **out_transaction, int64_t *out_deleted_rows);

int32_t lance_dataset_add_columns(void *dataset,
                                  const ArrowSchema *new_columns_schema,
                                  const char **expressions,
                                  size_t expressions_len, uint32_t batch_size);
int32_t lance_dataset_drop_columns(void *dataset, const char **columns,
                                   size_t columns_len);
int32_t lance_dataset_alter_columns_rename(void *dataset, const char *path,
                                           const char *new_name);
int32_t lance_dataset_alter_columns_set_nullable(void *dataset,
                                                 const char *path,
                                                 uint8_t nullable);
int32_t lance_dataset_alter_columns_cast(void *dataset, const char *path,
                                         const ArrowSchema *new_type_schema);

int32_t lance_dataset_update_table_metadata(void *dataset, const char *key,
                                            const char *value);
int32_t lance_dataset_update_config(void *dataset, const char *key,
                                    const char *value);
int32_t lance_dataset_update_schema_metadata(void *dataset, const char *key,
                                             const char *value);
int32_t lance_dataset_update_field_metadata(void *dataset,
                                            const char *field_path,
                                            const char *key, const char *value);

int32_t lance_dataset_compact_files(void *dataset);
int32_t lance_dataset_compact_files_with_options(void *dataset,
                                                 const char *options_json,
                                                 const char **out_metrics_json);
int32_t lance_dataset_cleanup_old_versions(void *dataset,
                                           int64_t older_than_seconds,
                                           uint8_t delete_unverified);
int32_t lance_dataset_cleanup_old_versions_with_options(
    void *dataset, const char *options_json, const char **out_metrics_json);

const char *lance_dataset_list_config(void *dataset);
const char *lance_dataset_list_table_metadata(void *dataset);
const char *lance_dataset_list_schema_metadata(void *dataset);
const char *lance_dataset_list_field_metadata(void *dataset,
                                              const char *field_path);
const char *lance_dataset_list_indices(void *dataset);
int32_t lance_dataset_create_scalar_index(void *dataset, const char *column,
                                          const char *index_name,
                                          uint8_t replace);

uint64_t *lance_dataset_list_fragments(void *dataset, size_t *out_len);
void lance_free_fragment_list(uint64_t *ptr, size_t len);
typedef struct LanceFieldStats {
  uint32_t field_id;
  uint64_t bytes_on_disk;
} LanceFieldStats;

typedef struct LanceFragmentStats {
  uint64_t fragment_id;
  int64_t num_rows;
  uint64_t bytes_on_disk;
} LanceFragmentStats;

LanceFragmentStats *lance_dataset_list_fragment_stats(void *dataset,
                                                      size_t *out_len);
void lance_free_fragment_stats_list(LanceFragmentStats *ptr, size_t len);

#ifdef LANCE_VANE_DISTRIBUTED
LanceFragmentStats *
lance_dataset_list_distributed_fragment_stats(void *dataset, size_t *out_len);
#endif

LanceFieldStats *lance_dataset_list_field_stats(void *dataset, size_t *out_len);
void lance_free_field_stats_list(LanceFieldStats *ptr, size_t len);

typedef struct LanceNamedFieldStats {
  const char *name;
  uint64_t bytes_on_disk;
} LanceNamedFieldStats;

LanceNamedFieldStats *lance_dataset_list_named_field_stats(void *dataset,
                                                           size_t *out_len);
void lance_free_named_field_stats_list(LanceNamedFieldStats *ptr, size_t len);
void *lance_create_fragment_stream_ir(void *dataset, uint64_t fragment_id,
                                      const char **columns, size_t columns_len,
                                      const uint8_t *filter_ir,
                                      size_t filter_ir_len);
void *lance_create_dataset_stream_ir(void *dataset, const char **columns,
                                     size_t columns_len,
                                     const uint8_t *filter_ir,
                                     size_t filter_ir_len, int64_t limit,
                                     int64_t offset);
void *lance_create_dataset_sample_stream_ir(void *dataset, const char **columns,
                                            size_t columns_len,
                                            double sample_percentage,
                                            int64_t seed, uint8_t repeatable);
void *lance_create_dataset_take_stream(void *dataset, const uint64_t *row_ids,
                                       size_t row_ids_len, const char **columns,
                                       size_t columns_len);
void *lance_create_dataset_take_stream_unfiltered(void *dataset,
                                                  const uint64_t *row_ids,
                                                  size_t row_ids_len,
                                                  const char **columns,
                                                  size_t columns_len);

void *lance_open_writer_with_storage_options(
    const char *path, const char *mode, const char **option_keys,
    const char **option_values, size_t options_len, uint64_t max_rows_per_file,
    uint64_t max_rows_per_group, uint64_t max_bytes_per_file,
    const char *data_storage_version, void *session, const ArrowSchema *schema);
void *lance_open_uncommitted_writer_with_storage_options(
    const char *path, const char *mode, const char **option_keys,
    const char **option_values, size_t options_len, uint64_t max_rows_per_file,
    uint64_t max_rows_per_group, uint64_t max_bytes_per_file,
    const char *data_storage_version, void *session, const ArrowSchema *schema);
int32_t lance_writer_write_batch(void *writer, ArrowArray *array);
int32_t lance_writer_finish(void *writer);
int32_t lance_writer_finish_uncommitted(void *writer, void **out_transaction);
void lance_close_writer(void *writer);

int32_t lance_commit_transaction_with_storage_options(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, void *session, void *transaction);
void lance_free_transaction(void *transaction);

#ifdef LANCE_VANE_DISTRIBUTED
void *lance_open_distributed_uncommitted_writer_with_storage_options(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, uint64_t expected_version,
    const char *expected_generation, const char *expected_creation_uuid,
    const char *operation_id, const char *query_id, const char *task_attempt_id,
    uint64_t max_rows_per_file, uint64_t max_rows_per_group,
    uint64_t max_bytes_per_file, void *session, const void *schema);

void *lance_distributed_encode_append_transaction(
    void *transaction, uint64_t expected_version, const char *operation_id,
    const char *query_id, const char *task_attempt_id, uint64_t row_count);
void *lance_distributed_decode_append_transaction(
    const uint8_t *bytes, size_t bytes_len, uint64_t expected_version,
    const char *operation_id, const char *query_id, const char *task_attempt_id,
    uint64_t row_count);
const uint8_t *lance_distributed_transaction_bytes(void *transaction,
                                                   size_t *out_len);
size_t lance_distributed_transaction_artifact_count(void *transaction);
const char *lance_distributed_transaction_artifact_path(void *transaction,
                                                        size_t index);
uint64_t lance_distributed_transaction_artifact_size(void *transaction,
                                                     size_t index);
uint64_t lance_distributed_transaction_byte_count(void *transaction);
void lance_free_distributed_transaction(void *transaction);

void *lance_distributed_create_mutation_transaction(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, void *session, uint8_t mutation_kind,
    uint64_t expected_version, const char *expected_generation,
    const char *operation_id, const char *query_id, const char *task_attempt_id,
    const char *schema_fingerprint, const uint64_t *source_fragment_ids,
    size_t source_fragment_count, const uint64_t *row_ids, size_t row_id_count,
    const char **set_columns, const uint8_t **set_expr_irs,
    const size_t *set_expr_ir_lengths, size_t set_count,
    uint64_t max_rows_per_file, uint64_t max_rows_per_group,
    uint64_t max_bytes_per_file);
void *lance_distributed_decode_mutation_transaction(
    const uint8_t *bytes, size_t bytes_len, uint8_t mutation_kind,
    uint64_t expected_version, const char *operation_id, const char *query_id,
    const char *task_attempt_id, const char *schema_fingerprint,
    const uint64_t *source_fragment_ids, size_t source_fragment_count,
    uint64_t row_count);
const uint8_t *lance_distributed_mutation_transaction_bytes(void *transaction,
                                                            size_t *out_len);
size_t lance_distributed_mutation_transaction_artifact_count(void *transaction);
const char *
lance_distributed_mutation_transaction_artifact_path(void *transaction,
                                                     size_t index);
uint64_t lance_distributed_mutation_transaction_artifact_size(void *transaction,
                                                              size_t index);
uint64_t lance_distributed_mutation_transaction_byte_count(void *transaction);
void lance_free_distributed_mutation_transaction(void *transaction);

int32_t lance_distributed_commit_empty_create(const char *path,
                                              const char **option_keys,
                                              const char **option_values,
                                              size_t options_len, void *session,
                                              const char *operation_id,
                                              void *transaction);
int32_t lance_distributed_commit_append_transactions(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, void *session, uint64_t expected_version,
    const char *expected_generation, const char *operation_id,
    const uint8_t **transaction_bytes, const size_t *transaction_lengths,
    size_t transaction_count, uint8_t *out_commit_started);
int32_t lance_distributed_commit_mutation_transactions(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, void *session, uint8_t mutation_kind,
    uint64_t expected_version, const char *expected_generation,
    const char *operation_id, const char *schema_fingerprint,
    const uint64_t *source_fragment_ids, size_t source_fragment_count,
    const uint8_t **transaction_bytes, const size_t *transaction_lengths,
    size_t transaction_count, uint8_t *out_commit_started);
int32_t lance_distributed_publish_attempt_manifest(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, uint64_t expected_version, const char *operation_id,
    const char *query_id, const char *task_attempt_id,
    const uint8_t **transaction_bytes, const size_t *transaction_lengths,
    size_t transaction_count);
int32_t lance_distributed_cleanup_attempt_manifests(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, const char *operation_id,
    const char **retained_task_attempt_ids, size_t retained_task_attempt_count);
int32_t lance_distributed_release_attempt_manifests(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, const char *operation_id,
    const char **released_task_attempt_ids, size_t released_task_attempt_count);
int32_t lance_distributed_cleanup_append_transaction(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, const char *operation_id, const uint8_t *bytes,
    size_t bytes_len);
int32_t lance_distributed_publish_mutation_attempt_manifest(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, uint8_t mutation_kind, uint64_t expected_version,
    const char *expected_generation, const char *operation_id,
    const char *query_id, const char *task_attempt_id,
    const char *schema_fingerprint, const uint64_t *source_fragment_ids,
    size_t source_fragment_count, const uint8_t **transaction_bytes,
    const size_t *transaction_lengths, size_t transaction_count);
int32_t lance_distributed_cleanup_mutation_attempt_manifests(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, const char *operation_id,
    const char **retained_task_attempt_ids, size_t retained_task_attempt_count);
int32_t lance_distributed_release_mutation_attempt_manifests(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, const char *operation_id,
    const char **released_task_attempt_ids, size_t released_task_attempt_count);
int32_t lance_distributed_cleanup_mutation_transaction(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, const char *operation_id, const uint8_t *bytes,
    size_t bytes_len);
int32_t lance_distributed_cleanup_append_transaction_handle(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, void *transaction);
int32_t lance_distributed_abort_uncommitted_writer(
    void *writer, const char *path, const char **option_keys,
    const char **option_values, size_t options_len, uint64_t expected_version,
    const char *operation_id, const char *query_id,
    const char *task_attempt_id);
#endif

int32_t lance_overwrite_update_transaction_with_irs_and_storage_options(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, const uint8_t *predicate_ir, size_t predicate_ir_len,
    const char **set_columns, const uint8_t **set_expr_irs,
    const size_t *set_expr_ir_lens, size_t set_len, uint64_t max_rows_per_file,
    uint64_t max_rows_per_group, uint64_t max_bytes_per_file, void *session,
    void **out_transaction, uint64_t *out_rows_updated);

int32_t lance_merge_begin_with_storage_options(
    const char *path, const char **option_keys, const char **option_values,
    size_t options_len, uint64_t max_rows_per_file, uint64_t max_rows_per_group,
    uint64_t max_bytes_per_file, void *session, void **out_merge_handle);
int32_t lance_merge_add_delete_rowids(void *merge_handle,
                                      const uint64_t *row_ids,
                                      size_t row_ids_len);
int32_t lance_merge_add_insert_batch(void *merge_handle, void *array);
int32_t lance_merge_finish_uncommitted(void *merge_handle,
                                       void **out_transaction);
void lance_merge_abort(void *merge_handle);

const char *lance_explain_dataset_scan_ir(void *dataset, const char **columns,
                                          size_t columns_len,
                                          const uint8_t *filter_ir,
                                          size_t filter_ir_len, int64_t limit,
                                          int64_t offset, uint8_t verbose);

void *lance_get_knn_schema(void *dataset, const char *vector_column,
                           const float *query_values, size_t query_len,
                           uint64_t k, uint64_t nprobes, uint64_t refine_factor,
                           uint8_t prefilter, uint8_t use_index);
void *lance_create_knn_stream_ir(void *dataset, const char *vector_column,
                                 const float *query_values, size_t query_len,
                                 uint64_t k, uint64_t nprobes,
                                 uint64_t refine_factor,
                                 const uint8_t *filter_ir, size_t filter_ir_len,
                                 uint8_t prefilter, uint8_t use_index);

const char *lance_explain_knn_scan_ir(void *dataset, const char *vector_column,
                                      const float *query_values,
                                      size_t query_len, uint64_t k,
                                      uint64_t nprobes, uint64_t refine_factor,
                                      const uint8_t *filter_ir,
                                      size_t filter_ir_len, uint8_t prefilter,
                                      uint8_t use_index, uint8_t verbose);

void *lance_get_fts_schema(void *dataset, const char *text_column,
                           const char *query, uint64_t k, uint8_t prefilter);
void *lance_create_fts_stream_ir(void *dataset, const char *text_column,
                                 const char *query, uint64_t k,
                                 const uint8_t *filter_ir, size_t filter_ir_len,
                                 uint8_t prefilter);

typedef struct LanceNamespaceQueryConfig {
  uint8_t namespace_kind;
  const char *root;
  const char **option_keys;
  const char **option_values;
  size_t options_len;
  const char *endpoint;
  const char *table_id;
  const char *bearer_token;
  const char *api_key;
  const char *delimiter;
  const char *headers_tsv;
  const char **columns;
  size_t columns_len;
  const char *filter;
  uint64_t k;
#ifdef LANCE_VANE_DISTRIBUTED
  int64_t version;
#endif
  uint8_t prefilter;
} LanceNamespaceQueryConfig;

typedef struct LanceNamespaceVectorSearchOptions {
  const char *vector_column;
  const float *query_values;
  size_t query_len;
  uint64_t nprobes;
  uint64_t refine_factor;
  uint8_t use_index;
} LanceNamespaceVectorSearchOptions;

typedef struct LanceNamespaceFtsSearchOptions {
  const char *text_column;
  const char *query;
} LanceNamespaceFtsSearchOptions;

void *lance_create_namespace_vector_search_stream(
    const LanceNamespaceQueryConfig *config,
    const LanceNamespaceVectorSearchOptions *options);
void *lance_create_namespace_fts_search_stream(
    const LanceNamespaceQueryConfig *config,
    const LanceNamespaceFtsSearchOptions *options);
void *lance_create_namespace_scan_stream_ir(
    const LanceNamespaceQueryConfig *config, const uint8_t *filter_ir,
    size_t filter_ir_len, int64_t limit, int64_t offset, uint8_t with_row_id);

void *lance_get_hybrid_schema(void *dataset);
void *lance_create_hybrid_stream_ir(
    void *dataset, const char *vector_column, const float *query_values,
    size_t query_len, const char *text_column, const char *text_query,
    uint64_t k, uint64_t nprobes, uint64_t refine_factor,
    const uint8_t *filter_ir, size_t filter_ir_len, uint8_t prefilter,
    uint8_t use_index, float alpha, uint32_t oversample_factor);

// Index DDL / metadata
int32_t lance_dataset_create_index(void *dataset, const char *index_name,
                                   const char **columns, size_t columns_len,
                                   const char *index_type,
                                   const char *params_json, uint8_t replace,
                                   uint8_t train);
int32_t lance_dataset_drop_index(void *dataset, const char *index_name);
int32_t lance_dataset_optimize_index(void *dataset, const char *index_name,
                                     uint8_t retrain);
int32_t
lance_dataset_optimize_index_with_options(void *dataset, const char *index_name,
                                          const char *options_json,
                                          const char **out_metrics_json);
void *lance_get_index_list_schema(void *dataset);
void *lance_create_index_list_stream(void *dataset);
char **lance_dataset_list_scalar_indexed_columns(void *dataset,
                                                 size_t *out_len);
void lance_free_scalar_indexed_columns(char **ptr, size_t len);

void lance_free_batch(void *batch);
int32_t lance_batch_to_arrow(void *batch, ArrowArray *out_array,
                             ArrowSchema *out_schema);
}
