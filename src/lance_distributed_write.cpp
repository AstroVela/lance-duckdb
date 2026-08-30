#ifdef LANCE_VANE_DISTRIBUTED

#include "lance_distributed_write.hpp"

#include "lance_common.hpp"
#include "lance_dataset_cache.hpp"
#include "lance_ffi.hpp"
#include "lance_session_state.hpp"
#include "lance_table_entry.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/operator/scan/physical_empty_result.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/distributed_write.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <map>
#include <mutex>

namespace duckdb {

namespace {

static constexpr uint32_t LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION = 3;
static constexpr const char *LANCE_DISTRIBUTED_INSERT_OPERATOR = "insert";
static constexpr const char *LANCE_DISTRIBUTED_CTAS_OPERATOR = "ctas";
static constexpr const char *LANCE_DISTRIBUTED_WRITE_FRAGMENT_CODEC =
    "lance.append-transaction";
static constexpr const char *LANCE_DISTRIBUTED_DATA_ARTIFACT_CODEC =
    "lance.data-file";

enum class LanceDistributedWriteKind : uint8_t { INSERT = 1, CTAS = 2 };

struct LanceDistributedWriteTransport {
  LanceDistributedWriteKind write_kind = LanceDistributedWriteKind::INSERT;
  string operation_id;
  string catalog_name;
  string schema_name;
  string table_name;
  string dataset_uri;
  uint64_t expected_version = 0;
  string generation_id;
  string creation_uuid;
  string schema_fingerprint;
  vector<string> input_names;
  vector<LogicalType> input_types;
  vector<uint32_t> target_schema_field_depths;
  vector<uint8_t> target_schema_field_nullable;
  string data_storage_version;
};

struct LanceArrowNullability {
  vector<uint32_t> field_depths;
  vector<uint8_t> field_nullable;
};

struct LanceFrozenDirectoryTargetSnapshot {
  uint64_t version = 0;
  string generation_id;
  LanceArrowNullability nullability;
};

struct LanceDistributedCommitEnvelope {
  string operation_id;
  string dataset_uri;
  uint64_t expected_version = 0;
  string generation_id;
  string creation_uuid;
  string schema_fingerprint;
  string query_id;
  string task_attempt_id;
  string serialized_transport;
  vector<string> transactions;
  vector<uint64_t> transaction_row_counts;
  vector<uint64_t> transaction_byte_counts;
  vector<string> artifact_paths;
  vector<uint64_t> artifact_sizes;
  idx_t row_count = 0;
  idx_t byte_count = 0;
};

struct LanceDecodedDistributedCommit {
  vector<string> transactions;
  vector<string> manifest_task_attempt_ids;
  idx_t row_count = 0;
  idx_t byte_count = 0;
};

static idx_t CheckedAdd(idx_t left, idx_t right, const char *description) {
  if (right > NumericLimits<idx_t>::Maximum() - left) {
    throw InvalidInputException("Distributed Lance write %s exceeds idx_t",
                                description);
  }
  return left + right;
}

static bool IsCanonicalUUID(const string &value) {
  hugeint_t parsed;
  return value.size() == BaseUUID::STRING_SIZE &&
         UUID::FromString(value, parsed, true);
}

static void ValidateVaneTaskIdentityComponent(const string &component,
                                              const char *description) {
  if (component.empty() || (component.size() > 1 && component[0] == '0')) {
    throw InvalidInputException(
        "Distributed Lance write has an invalid Vane %s", description);
  }
  for (const auto character : component) {
    if (!StringUtil::CharacterIsDigit(character)) {
      throw InvalidInputException(
          "Distributed Lance write has an invalid Vane %s", description);
    }
  }
}

static string VaneLogicalTaskIdentity(const string &query_id,
                                      const string &task_attempt_id) {
  const auto prefix = query_id + ".";
  if (query_id.empty() ||
      task_attempt_id.compare(0, prefix.size(), prefix) != 0) {
    throw InvalidInputException(
        "Distributed Lance write task attempt does not match its Vane query "
        "identity");
  }
  const auto components =
      StringUtil::Split(task_attempt_id.substr(prefix.size()), '.');
  if (components.size() != 3) {
    throw InvalidInputException(
        "Distributed Lance write has an invalid Vane task attempt identity");
  }
  ValidateVaneTaskIdentityComponent(components[0],
                                    "fragment execution identity");
  ValidateVaneTaskIdentityComponent(components[1], "task partition identity");
  ValidateVaneTaskIdentityComponent(components[2], "task attempt identity");
  return prefix + components[0] + "." + components[1];
}

static const char *WriteOperatorName(LanceDistributedWriteKind write_kind) {
  switch (write_kind) {
  case LanceDistributedWriteKind::INSERT:
    return LANCE_DISTRIBUTED_INSERT_OPERATOR;
  case LanceDistributedWriteKind::CTAS:
    return LANCE_DISTRIBUTED_CTAS_OPERATOR;
  default:
    throw SerializationException(
        "Distributed Lance write bind has an invalid write kind");
  }
}

static vector<uint8_t>
TargetTopLevelNullability(const ColumnList &columns,
                          const vector<unique_ptr<Constraint>> &constraints) {
  unordered_set<idx_t> required_columns;
  for (const auto &constraint : constraints) {
    if (constraint->type == ConstraintType::NOT_NULL) {
      required_columns.insert(
          constraint->Cast<NotNullConstraint>().index.index);
    }
  }

  vector<uint8_t> result;
  result.reserve(columns.PhysicalColumnCount());
  for (idx_t index = 0; index < columns.PhysicalColumnCount(); index++) {
    const auto logical_index =
        columns.PhysicalToLogical(PhysicalIndex(index)).index;
    result.push_back(required_columns.count(logical_index) == 0 ? 1 : 0);
  }
  return result;
}

static idx_t ArrowSchemaChildCount(const ArrowSchema &schema) {
  if (schema.n_children < 0 || (schema.n_children > 0 && !schema.children)) {
    throw InternalException(
        "Distributed Lance Arrow schema has invalid children");
  }
  return static_cast<idx_t>(schema.n_children);
}

static void AppendArrowSchemaNullability(const ArrowSchema &parent,
                                         uint32_t depth,
                                         LanceArrowNullability &result) {
  const auto child_count = ArrowSchemaChildCount(parent);
  for (idx_t index = 0; index < child_count; index++) {
    auto *field = parent.children[index];
    if (!field) {
      throw InternalException(
          "Distributed Lance Arrow schema has a null field");
    }
    result.field_depths.push_back(depth);
    result.field_nullable.push_back(
        (field->flags & ARROW_FLAG_NULLABLE) != 0 ? 1 : 0);
    if (ArrowSchemaChildCount(*field) > 0) {
      if (depth == NumericLimits<uint32_t>::Maximum()) {
        throw InternalException(
            "Distributed Lance Arrow schema nesting is too deep");
      }
      AppendArrowSchemaNullability(*field, depth + 1, result);
    }
  }
}

static LanceArrowNullability
CaptureArrowSchemaNullability(const ArrowSchema &schema) {
  LanceArrowNullability result;
  AppendArrowSchemaNullability(schema, 0, result);
  return result;
}

static LanceArrowNullability CaptureDatasetSchemaNullability(void *dataset,
                                                             const string &path,
                                                             bool redact) {
  auto *schema_handle = lance_get_schema(dataset);
  if (!schema_handle) {
    throw IOException("Failed to get distributed Lance target schema" +
                      LanceVaneFormatErrorSuffix(path, redact));
  }
  ArrowSchemaWrapper schema_root;
  memset(&schema_root.arrow_schema, 0, sizeof(schema_root.arrow_schema));
  if (lance_schema_to_arrow(schema_handle, &schema_root.arrow_schema) != 0) {
    lance_free_schema(schema_handle);
    throw IOException("Failed to export distributed Lance target schema" +
                      LanceVaneFormatErrorSuffix(path, redact));
  }
  lance_free_schema(schema_handle);
  return CaptureArrowSchemaNullability(schema_root.arrow_schema);
}

static void
ApplyTopLevelFieldNullability(ArrowSchema &schema,
                              const vector<uint8_t> &target_field_nullable) {
  if (schema.n_children < 0 ||
      static_cast<idx_t>(schema.n_children) != target_field_nullable.size()) {
    throw InternalException(
        "Distributed Lance Arrow schema has an invalid field count");
  }
  for (idx_t index = 0; index < target_field_nullable.size(); index++) {
    auto *field = schema.children[index];
    if (!field) {
      throw InternalException(
          "Distributed Lance Arrow schema has a null field");
    }
    if (target_field_nullable[index] != 0) {
      field->flags |= ARROW_FLAG_NULLABLE;
    } else {
      field->flags &= ~ARROW_FLAG_NULLABLE;
    }
  }
}

static void ApplyArrowSchemaNullability(ArrowSchema &parent, uint32_t depth,
                                        const vector<uint32_t> &field_depths,
                                        const vector<uint8_t> &field_nullable,
                                        idx_t &position) {
  const auto child_count = ArrowSchemaChildCount(parent);
  for (idx_t index = 0; index < child_count; index++) {
    if (position >= field_depths.size() || field_depths[position] != depth) {
      throw SerializationException(
          "Distributed Lance target nullability does not match its Arrow "
          "schema shape");
    }
    auto *field = parent.children[index];
    if (!field) {
      throw InternalException(
          "Distributed Lance Arrow schema has a null field");
    }
    if (field_nullable[position] != 0) {
      field->flags |= ARROW_FLAG_NULLABLE;
    } else {
      field->flags &= ~ARROW_FLAG_NULLABLE;
    }
    position++;
    if (ArrowSchemaChildCount(*field) > 0) {
      if (depth == NumericLimits<uint32_t>::Maximum()) {
        throw SerializationException(
            "Distributed Lance target schema nesting is too deep");
      }
      ApplyArrowSchemaNullability(*field, depth + 1, field_depths,
                                  field_nullable, position);
    }
  }
}

static void
ApplyTargetSchemaNullability(ArrowSchema &schema,
                             const vector<uint32_t> &field_depths,
                             const vector<uint8_t> &field_nullable) {
  if (field_depths.size() != field_nullable.size()) {
    throw SerializationException(
        "Distributed Lance target nullability has mismatched field state");
  }
  idx_t position = 0;
  ApplyArrowSchemaNullability(schema, 0, field_depths, field_nullable,
                              position);
  if (position != field_depths.size()) {
    throw SerializationException(
        "Distributed Lance target nullability has trailing field state");
  }
}

static string SchemaFingerprint(const vector<string> &names,
                                const vector<LogicalType> &types,
                                const vector<uint32_t> &field_depths,
                                const vector<uint8_t> &field_nullable,
                                const string &data_storage_version) {
  MemoryStream stream(Allocator::DefaultAllocator());
  BinarySerializer serializer(stream);
  serializer.Begin();
  serializer.WriteProperty(1, "field_names", names);
  serializer.WriteProperty(2, "field_types", types);
  serializer.WriteProperty(3, "target_schema_field_depths", field_depths);
  serializer.WriteProperty(4, "target_schema_field_nullable", field_nullable);
  serializer.WriteProperty(5, "data_storage_version", data_storage_version);
  serializer.End();
  MD5Context context;
  context.Add(stream.GetData(), stream.GetPosition());
  return context.FinishHex();
}

static void ValidateTransport(const LanceDistributedWriteTransport &transport) {
  if (!IsCanonicalUUID(transport.operation_id) ||
      transport.catalog_name.empty() || transport.schema_name.empty() ||
      transport.table_name.empty() || transport.dataset_uri.empty() ||
      transport.expected_version == 0 ||
      transport.schema_fingerprint.size() != 32 ||
      transport.input_names.empty() ||
      transport.input_names.size() != transport.input_types.size() ||
      transport.target_schema_field_depths.empty() ||
      transport.target_schema_field_depths.size() !=
          transport.target_schema_field_nullable.size()) {
    throw SerializationException(
        "Distributed Lance write bind has incomplete target state");
  }
  (void)WriteOperatorName(transport.write_kind);
  if (transport.write_kind == LanceDistributedWriteKind::INSERT) {
    if (transport.generation_id.empty() || !transport.creation_uuid.empty()) {
      throw SerializationException(
          "Distributed Lance INSERT bind has invalid generation state");
    }
  } else if (!transport.generation_id.empty() ||
             transport.creation_uuid != transport.operation_id ||
             transport.expected_version != 1 ||
             transport.data_storage_version.empty()) {
    throw SerializationException(
        "Distributed Lance CTAS bind has invalid prepared-generation state");
  }
  unordered_set<string> names;
  for (idx_t index = 0; index < transport.input_names.size(); index++) {
    if (transport.input_names[index].empty() ||
        transport.input_types[index].id() == LogicalTypeId::INVALID ||
        !names.insert(transport.input_names[index]).second) {
      throw SerializationException(
          "Distributed Lance write bind has an invalid input schema");
    }
  }
  idx_t top_level_fields = 0;
  for (idx_t index = 0; index < transport.target_schema_field_depths.size();
       index++) {
    const auto depth = transport.target_schema_field_depths[index];
    if (transport.target_schema_field_nullable[index] > 1 ||
        (index == 0 && depth != 0) ||
        (index > 0 && depth > transport.target_schema_field_depths[index - 1] &&
         depth - transport.target_schema_field_depths[index - 1] > 1)) {
      throw SerializationException(
          "Distributed Lance write bind has invalid target nullability");
    }
    if (depth == 0) {
      top_level_fields++;
    }
  }
  if (top_level_fields != transport.input_names.size()) {
    throw SerializationException(
        "Distributed Lance write bind target nullability has an invalid "
        "top-level field count");
  }
}

static string
SerializeTransport(const LanceDistributedWriteTransport &transport) {
  ValidateTransport(transport);
  MemoryStream stream(Allocator::DefaultAllocator());
  BinarySerializer serializer(stream);
  serializer.Begin();
  serializer.WriteProperty(1, "protocol_version",
                           LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION);
  serializer.WriteProperty(2, "write_kind",
                           static_cast<uint8_t>(transport.write_kind));
  serializer.WriteProperty(3, "operation_id", transport.operation_id);
  serializer.WriteProperty(4, "catalog_name", transport.catalog_name);
  serializer.WriteProperty(5, "schema_name", transport.schema_name);
  serializer.WriteProperty(6, "table_name", transport.table_name);
  serializer.WriteProperty(7, "dataset_uri", transport.dataset_uri);
  serializer.WriteProperty(8, "expected_version", transport.expected_version);
  serializer.WriteProperty(9, "generation_id", transport.generation_id);
  serializer.WriteProperty(10, "creation_uuid", transport.creation_uuid);
  serializer.WriteProperty(11, "schema_fingerprint",
                           transport.schema_fingerprint);
  serializer.WriteProperty(12, "input_names", transport.input_names);
  serializer.WriteProperty(13, "input_types", transport.input_types);
  serializer.WriteProperty(14, "data_storage_version",
                           transport.data_storage_version);
  serializer.WriteProperty(15, "target_schema_field_depths",
                           transport.target_schema_field_depths);
  serializer.WriteProperty(16, "target_schema_field_nullable",
                           transport.target_schema_field_nullable);
  serializer.End();
  return string(reinterpret_cast<const char *>(stream.GetData()),
                stream.GetPosition());
}

static LanceDistributedWriteTransport
DeserializeTransport(const string &bytes) {
  if (bytes.empty()) {
    throw SerializationException(
        "Cannot deserialize an empty distributed Lance write bind");
  }
  vector<data_t> buffer(bytes.begin(), bytes.end());
  MemoryStream stream(buffer.data(), buffer.size());
  BinaryDeserializer deserializer(stream);
  deserializer.Begin();
  const auto protocol_version =
      deserializer.ReadProperty<uint32_t>(1, "protocol_version");
  if (protocol_version != LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION) {
    throw SerializationException(
        "Distributed Lance write bind has unsupported protocol version %u",
        protocol_version);
  }
  LanceDistributedWriteTransport result;
  result.write_kind = static_cast<LanceDistributedWriteKind>(
      deserializer.ReadProperty<uint8_t>(2, "write_kind"));
  result.operation_id = deserializer.ReadProperty<string>(3, "operation_id");
  result.catalog_name = deserializer.ReadProperty<string>(4, "catalog_name");
  result.schema_name = deserializer.ReadProperty<string>(5, "schema_name");
  result.table_name = deserializer.ReadProperty<string>(6, "table_name");
  result.dataset_uri = deserializer.ReadProperty<string>(7, "dataset_uri");
  result.expected_version =
      deserializer.ReadProperty<uint64_t>(8, "expected_version");
  result.generation_id = deserializer.ReadProperty<string>(9, "generation_id");
  result.creation_uuid = deserializer.ReadProperty<string>(10, "creation_uuid");
  result.schema_fingerprint =
      deserializer.ReadProperty<string>(11, "schema_fingerprint");
  result.input_names =
      deserializer.ReadProperty<vector<string>>(12, "input_names");
  result.input_types =
      deserializer.ReadProperty<vector<LogicalType>>(13, "input_types");
  result.data_storage_version =
      deserializer.ReadProperty<string>(14, "data_storage_version");
  result.target_schema_field_depths =
      deserializer.ReadProperty<vector<uint32_t>>(15,
                                                  "target_schema_field_depths");
  result.target_schema_field_nullable =
      deserializer.ReadProperty<vector<uint8_t>>(
          16, "target_schema_field_nullable");
  deserializer.End();
  ValidateTransport(result);
  return result;
}

static string
SerializeCommitEnvelope(const LanceDistributedCommitEnvelope &envelope) {
  MemoryStream stream(Allocator::DefaultAllocator());
  BinarySerializer serializer(stream);
  serializer.Begin();
  serializer.WriteProperty(1, "protocol_version",
                           LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION);
  serializer.WriteProperty(2, "operation_id", envelope.operation_id);
  serializer.WriteProperty(3, "dataset_uri", envelope.dataset_uri);
  serializer.WriteProperty(4, "expected_version", envelope.expected_version);
  serializer.WriteProperty(5, "generation_id", envelope.generation_id);
  serializer.WriteProperty(6, "creation_uuid", envelope.creation_uuid);
  serializer.WriteProperty(7, "schema_fingerprint",
                           envelope.schema_fingerprint);
  serializer.WriteProperty(8, "query_id", envelope.query_id);
  serializer.WriteProperty(9, "task_attempt_id", envelope.task_attempt_id);
  serializer.WriteProperty(10, "serialized_transport",
                           envelope.serialized_transport);
  serializer.WriteProperty(11, "transactions", envelope.transactions);
  serializer.WriteProperty(12, "transaction_row_counts",
                           envelope.transaction_row_counts);
  serializer.WriteProperty(13, "transaction_byte_counts",
                           envelope.transaction_byte_counts);
  serializer.WriteProperty(14, "artifact_paths", envelope.artifact_paths);
  serializer.WriteProperty(15, "artifact_sizes", envelope.artifact_sizes);
  serializer.WriteProperty(16, "row_count", envelope.row_count);
  serializer.WriteProperty(17, "byte_count", envelope.byte_count);
  serializer.End();
  return string(reinterpret_cast<const char *>(stream.GetData()),
                stream.GetPosition());
}

static LanceDistributedCommitEnvelope
DeserializeCommitEnvelope(const string &bytes) {
  if (bytes.empty()) {
    throw SerializationException(
        "Cannot deserialize an empty distributed Lance commit fragment");
  }
  vector<data_t> buffer(bytes.begin(), bytes.end());
  MemoryStream stream(buffer.data(), buffer.size());
  BinaryDeserializer deserializer(stream);
  deserializer.Begin();
  const auto protocol_version =
      deserializer.ReadProperty<uint32_t>(1, "protocol_version");
  if (protocol_version != LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION) {
    throw SerializationException(
        "Distributed Lance commit fragment has unsupported protocol version "
        "%u",
        protocol_version);
  }
  LanceDistributedCommitEnvelope result;
  result.operation_id = deserializer.ReadProperty<string>(2, "operation_id");
  result.dataset_uri = deserializer.ReadProperty<string>(3, "dataset_uri");
  result.expected_version =
      deserializer.ReadProperty<uint64_t>(4, "expected_version");
  result.generation_id = deserializer.ReadProperty<string>(5, "generation_id");
  result.creation_uuid = deserializer.ReadProperty<string>(6, "creation_uuid");
  result.schema_fingerprint =
      deserializer.ReadProperty<string>(7, "schema_fingerprint");
  result.query_id = deserializer.ReadProperty<string>(8, "query_id");
  result.task_attempt_id =
      deserializer.ReadProperty<string>(9, "task_attempt_id");
  result.serialized_transport =
      deserializer.ReadProperty<string>(10, "serialized_transport");
  result.transactions =
      deserializer.ReadProperty<vector<string>>(11, "transactions");
  result.transaction_row_counts =
      deserializer.ReadProperty<vector<uint64_t>>(12, "transaction_row_counts");
  result.transaction_byte_counts = deserializer.ReadProperty<vector<uint64_t>>(
      13, "transaction_byte_counts");
  result.artifact_paths =
      deserializer.ReadProperty<vector<string>>(14, "artifact_paths");
  result.artifact_sizes =
      deserializer.ReadProperty<vector<uint64_t>>(15, "artifact_sizes");
  result.row_count = deserializer.ReadProperty<idx_t>(16, "row_count");
  result.byte_count = deserializer.ReadProperty<idx_t>(17, "byte_count");
  deserializer.End();
  if (!IsCanonicalUUID(result.operation_id) || result.dataset_uri.empty() ||
      result.expected_version == 0 || result.schema_fingerprint.size() != 32 ||
      result.query_id.empty() || result.task_attempt_id.empty() ||
      result.serialized_transport.empty() || result.transactions.empty() ||
      result.transactions.size() != result.transaction_row_counts.size() ||
      result.transactions.size() != result.transaction_byte_counts.size() ||
      result.artifact_paths.empty() ||
      result.artifact_paths.size() != result.artifact_sizes.size() ||
      result.row_count == 0) {
    throw SerializationException(
        "Distributed Lance commit fragment contains invalid identity or "
        "payload state");
  }
  return result;
}

struct LanceDatasetHandleDeleter {
  void operator()(void *dataset) const {
    if (dataset) {
      lance_close_dataset(dataset);
    }
  }
};

struct LanceCStringDeleter {
  void operator()(const char *value) const {
    if (value) {
      lance_free_string(value);
    }
  }
};

static string DatasetGenerationId(ClientContext &context, const string &path,
                                  void *dataset) {
  auto *generation = lance_dataset_generation_id(dataset);
  if (!generation) {
    throw IOException("Failed to identify distributed Lance target" +
                      LanceVaneFormatErrorSuffix(
                          path, LanceVanePathRequiresRedaction(context, path)));
  }
  unique_ptr<const char, LanceCStringDeleter> owner(generation);
  string result = generation;
  if (result.empty()) {
    throw IOException("Distributed Lance target generation is empty");
  }
  return result;
}

static pair<uint64_t, string> CaptureTargetSnapshot(ClientContext &context,
                                                    const string &path) {
  unique_ptr<void, LanceDatasetHandleDeleter> dataset(
      LanceOpenDatasetForDistributedScan(context, path));
  if (!dataset) {
    throw IOException("Failed to open distributed Lance target" +
                      LanceVaneFormatErrorSuffix(
                          path, LanceVanePathRequiresRedaction(context, path)));
  }
  const auto version = lance_dataset_version(dataset.get());
  if (version == 0) {
    throw IOException("Failed to resolve distributed Lance target version" +
                      LanceVaneFormatErrorSuffix(
                          path, LanceVanePathRequiresRedaction(context, path)));
  }
  return {version, DatasetGenerationId(context, path, dataset.get())};
}

static LanceFrozenDirectoryTargetSnapshot
CaptureFrozenDirectoryTargetSnapshot(const string &path,
                                     const vector<string> &option_keys,
                                     const vector<string> &option_values) {
  if (option_keys.size() != option_values.size()) {
    throw InternalException(
        "Distributed Lance target has mismatched storage options");
  }
  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                  value_ptrs);
  unique_ptr<void, LanceDatasetHandleDeleter> dataset(
      lance_open_dataset_with_storage_options(
          path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
          value_ptrs.empty() ? nullptr : value_ptrs.data(),
          option_keys.size()));
  const auto redact = !option_keys.empty() || LanceVanePathIsRemote(path) ||
                      LanceVanePathHasPrivateUriComponents(path);
  if (!dataset) {
    throw IOException("Failed to open distributed Lance target" +
                      LanceVaneFormatErrorSuffix(path, redact));
  }
  const auto version = lance_dataset_version(dataset.get());
  if (version == 0) {
    throw IOException("Failed to resolve distributed Lance target version" +
                      LanceVaneFormatErrorSuffix(path, redact));
  }
  auto *generation = lance_dataset_generation_id(dataset.get());
  if (!generation) {
    throw IOException("Failed to identify distributed Lance target" +
                      LanceVaneFormatErrorSuffix(path, redact));
  }
  unique_ptr<const char, LanceCStringDeleter> generation_owner(generation);
  string generation_id = generation;
  if (generation_id.empty()) {
    throw IOException("Distributed Lance target generation is empty");
  }
  LanceFrozenDirectoryTargetSnapshot result;
  result.version = version;
  result.generation_id = std::move(generation_id);
  result.nullability =
      CaptureDatasetSchemaNullability(dataset.get(), path, redact);
  return result;
}

static void ValidateTargetSnapshot(ClientContext &context,
                                   const LanceDistributedWriteTransport &target,
                                   const string &expected_generation) {
  auto current = CaptureTargetSnapshot(context, target.dataset_uri);
  if (current.first != target.expected_version ||
      current.second != expected_generation) {
    throw TransactionException(
        "Lance table %s.%s.%s changed after the distributed write was "
        "planned",
        target.catalog_name, target.schema_name, target.table_name);
  }
}

static bool IsSafeDatasetTableName(const string &name) {
  return !name.empty() && name != "." && name != ".." &&
         name.find('/') == string::npos && name.find('\\') == string::npos;
}

static string JoinNamespacePath(const string &root, const string &child) {
  if (root.empty()) {
    return child;
  }
  if (root.back() == '/' || root.back() == '\\') {
    return root + child;
  }
  return root + "/" + child;
}

static bool
DirectorySessionMatches(ClientContext &context, const string &root,
                        const vector<string> &attached_option_keys,
                        const vector<string> &attached_option_values) {
  string current_root;
  vector<string> current_keys;
  vector<string> current_values;
  ResolveLanceStorageOptionsForDistributedRead(context, root, current_root,
                                               current_keys, current_values);
  if (current_root != root || current_keys.size() != current_values.size() ||
      attached_option_keys.size() != attached_option_values.size()) {
    return false;
  }
  vector<pair<string, string>> current_options;
  vector<pair<string, string>> attached_options;
  for (idx_t index = 0; index < current_keys.size(); index++) {
    current_options.emplace_back(current_keys[index], current_values[index]);
  }
  for (idx_t index = 0; index < attached_option_keys.size(); index++) {
    attached_options.emplace_back(attached_option_keys[index],
                                  attached_option_values[index]);
  }
  std::sort(current_options.begin(), current_options.end());
  std::sort(attached_options.begin(), attached_options.end());
  return current_options == attached_options;
}

static bool DirectoryTableExists(const string &root, const string &table_name,
                                 const vector<string> &option_keys,
                                 const vector<string> &option_values) {
  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                  value_ptrs);
  auto *tables = lance_dir_namespace_list_tables(
      root.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
      value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size());
  if (!tables) {
    throw IOException(
        "Failed to list tables for distributed Lance CTAS" +
        LanceVaneFormatErrorSuffix(root, !option_keys.empty() ||
                                             LanceVanePathIsRemote(root)));
  }
  unique_ptr<const char, LanceCStringDeleter> owner(tables);
  for (const auto &entry : StringUtil::Split(string(tables), '\n')) {
    if (StringUtil::CIEquals(entry, table_name)) {
      return true;
    }
  }
  return false;
}

static void ResolveWorkerStorageOptions(ClientContext &context,
                                        const string &dataset_uri,
                                        string &open_path,
                                        vector<string> &option_keys,
                                        vector<string> &option_values) {
  ResolveLanceStorageOptionsForDistributedRead(context, dataset_uri, open_path,
                                               option_keys, option_values);
  if (open_path != dataset_uri) {
    throw InvalidInputException(
        "Distributed Lance worker storage resolution changed the frozen "
        "dataset URI");
  }
}

static void
ValidateResolvedInfo(const DistributedExtensionWriteInfo &info,
                     const LanceDistributedWriteTransport &transport) {
  if (info.mode != DistributedWriteMode::CALLBACK ||
      info.capability.extension_name != "lance" ||
      info.capability.capability.name !=
          WriteOperatorName(transport.write_kind) ||
      info.capability.capability.protocol_version !=
          LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION ||
      info.fragment_codec.name != LANCE_DISTRIBUTED_WRITE_FRAGMENT_CODEC ||
      info.fragment_codec.version != LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION) {
    throw InvalidInputException(
        "Distributed Lance write contract does not match its registration");
  }
}

static void
DecodeCommitResults(const DistributedExtensionWriteInfo &info,
                    const LanceDistributedWriteTransport &transport,
                    const vector<DistributedWriteTaskResult> &results,
                    LanceDecodedDistributedCommit &decoded);

} // namespace

class LanceDistributedWriteProvider::Impl {
public:
  explicit Impl(ClientContext &context_p) : context(&context_p) {}

  ClientContext *context;
  optional_ptr<LanceTableEntry> table;
  LanceDistributedWriteKind write_kind = LanceDistributedWriteKind::INSERT;
  vector<string> column_names;
  vector<LogicalType> column_types;
  vector<LogicalType> input_types;
  vector<uint8_t> target_top_level_nullable;
  string root;
  vector<string> attached_option_keys;
  vector<string> attached_option_values;
  bool uses_coordinator_storage_secret = false;
  bool distributed_replay_path_restricted = false;
  OnCreateConflict on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
  string catalog_name;
  string schema_name;
  string table_name;
  string data_storage_version;

  distributed::DistributedExtensionWritePlan plan;
  LanceDistributedWriteTransport transport;
  bool selected = false;
  mutable bool prepare_started = false;
  mutable bool prepared = false;
  mutable bool finalize_started = false;
  mutable string prepared_generation;

public:
  void Initialize();
  void ValidateShape() const;
  void Validate(ClientContext &context) const;
  void Prepare(ClientContext &context) const;
  idx_t Finalize(ClientContext &context,
                 const vector<DistributedWriteTaskResult> &results) const;
  void Abort(ClientContext &context,
             const vector<DistributedWriteTaskResult> &results) const;

private:
  string ResolveReplayPath(ClientContext &context, const string &path) const;
  void BestEffortCleanupTransactions(
      ClientContext &context,
      const LanceDecodedDistributedCommit &decoded) const noexcept;
  void CleanupAttemptManifests(
      ClientContext &context,
      const vector<string> &retained_task_attempt_ids) const;
  void BestEffortCleanupAttemptManifests(ClientContext &context) const noexcept;
  void BestEffortReleaseAttemptManifests(
      ClientContext &context,
      const vector<string> &released_task_attempt_ids) const noexcept;
  void InitializeInsert();
  void InitializeCTAS();
  void ValidateCTASAbsent() const;
  void PrepareCTAS(ClientContext &context) const;
};

namespace {

static void ValidateDistributedWriteAutoCommit(ClientContext &context) {
  if (!context.transaction.IsAutoCommit()) {
    throw NotImplementedException(
        "Distributed Lance writes require DuckDB auto-commit mode; use native "
        "DuckDB execution for explicit transactions");
  }
}

static void ValidateDirectoryReplayState(
    ClientContext &context, const string &root, const string &dataset_path,
    const vector<string> &option_keys, const vector<string> &option_values,
    bool uses_coordinator_storage_secret,
    bool distributed_replay_path_restricted) {
  if (uses_coordinator_storage_secret ||
      LanceHasMatchingStorageSecret(context, root) ||
      LanceHasMatchingStorageSecret(context, dataset_path)) {
    throw NotImplementedException(
        "Distributed Lance writes cannot use a coordinator-only TYPE LANCE "
        "secret; use replayable DuckDB session settings captured by Vane");
  }
  if (distributed_replay_path_restricted ||
      LanceVanePathHasPrivateUriComponents(root) ||
      LanceVanePathHasPrivateUriComponents(dataset_path)) {
    throw NotImplementedException(
        "Distributed Lance writes require a dataset URI without userinfo, "
        "query, or fragment components");
  }
  if (!DirectorySessionMatches(context, root, option_keys, option_values)) {
    throw NotImplementedException(
        "Distributed Lance directory writes require query-session storage "
        "settings to match the settings captured by ATTACH");
  }
}

static string CreateMode(OnCreateConflict on_conflict) {
  switch (on_conflict) {
  case OnCreateConflict::ERROR_ON_CONFLICT:
  case OnCreateConflict::IGNORE_ON_CONFLICT:
    return "create";
  case OnCreateConflict::REPLACE_ON_CONFLICT:
    return "overwrite";
  default:
    return "overwrite";
  }
}

} // namespace

string LanceDistributedWriteProvider::Impl::ResolveReplayPath(
    ClientContext &context, const string &path) const {
  auto replay_path = LanceVaneReplayPath(context, path);
  if (replay_path.empty()) {
    throw NotImplementedException(
        "Distributed Lance writes require a replayable shared dataset URI");
  }
  return replay_path;
}

void LanceDistributedWriteProvider::Impl::InitializeInsert() {
  if (!table) {
    throw InternalException(
        "Distributed Lance INSERT has no target table entry");
  }
  if (input_types != column_types) {
    throw InvalidInputException(
        "Distributed Lance INSERT input schema does not match the target "
        "table");
  }
  if (!table->IsNamespaceBacked()) {
    throw NotImplementedException(
        "Distributed Lance INSERT requires a directory-namespace table");
  }
  const auto &config = table->NamespaceConfig();
  if (config.IsRest()) {
    throw NotImplementedException(
        "Distributed Lance writes do not support REST namespace tables until "
        "the namespace exposes replayable physical write state");
  }

  root = config.root;
  attached_option_keys = config.option_keys;
  attached_option_values = config.option_values;
  uses_coordinator_storage_secret = config.uses_coordinator_storage_secret;
  distributed_replay_path_restricted =
      config.distributed_replay_path_restricted;
  auto dataset_path = LanceDirectoryNamespaceDatasetUri(config);
  if (uses_coordinator_storage_secret || distributed_replay_path_restricted ||
      LanceVanePathHasPrivateUriComponents(root) ||
      LanceVanePathHasPrivateUriComponents(dataset_path)) {
    throw NotImplementedException(
        "Distributed Lance writes require replayable session credentials and "
        "a dataset URI without private components");
  }
  dataset_path = ResolveReplayPath(*context, dataset_path);
  auto snapshot = CaptureFrozenDirectoryTargetSnapshot(
      dataset_path, attached_option_keys, attached_option_values);

  transport.write_kind = LanceDistributedWriteKind::INSERT;
  transport.operation_id = UUID::ToString(UUID::GenerateRandomUUID());
  transport.catalog_name = table->catalog.GetName();
  transport.schema_name = table->schema.name;
  transport.table_name = table->name;
  transport.dataset_uri = std::move(dataset_path);
  transport.expected_version = snapshot.version;
  transport.generation_id = std::move(snapshot.generation_id);
  transport.target_schema_field_depths =
      std::move(snapshot.nullability.field_depths);
  transport.target_schema_field_nullable =
      std::move(snapshot.nullability.field_nullable);
  transport.schema_fingerprint = SchemaFingerprint(
      column_names, column_types, transport.target_schema_field_depths,
      transport.target_schema_field_nullable, string());
  transport.input_names = column_names;
  transport.input_types = column_types;
}

void LanceDistributedWriteProvider::Impl::InitializeCTAS() {
  if (!IsSafeDatasetTableName(table_name) || root.empty() ||
      input_types != column_types) {
    throw InvalidInputException(
        "Distributed Lance CTAS has an invalid target or input schema");
  }
  const auto dataset_path = JoinNamespacePath(root, table_name + ".lance");
  if (uses_coordinator_storage_secret || distributed_replay_path_restricted ||
      LanceVanePathHasPrivateUriComponents(root) ||
      LanceVanePathHasPrivateUriComponents(dataset_path)) {
    throw NotImplementedException(
        "Distributed Lance writes require replayable session credentials and "
        "a dataset URI without private components");
  }

  transport.write_kind = LanceDistributedWriteKind::CTAS;
  transport.operation_id = UUID::ToString(UUID::GenerateRandomUUID());
  transport.catalog_name = catalog_name;
  transport.schema_name = schema_name;
  transport.table_name = table_name;
  transport.dataset_uri = ResolveReplayPath(*context, dataset_path);
  transport.expected_version = 1;
  transport.creation_uuid = transport.operation_id;
  ArrowSchemaWrapper schema_root;
  memset(&schema_root.arrow_schema, 0, sizeof(schema_root.arrow_schema));
  auto properties = context->GetClientProperties();
  ArrowConverter::ToArrowSchema(&schema_root.arrow_schema, column_types,
                                column_names, properties);
  ApplyTopLevelFieldNullability(schema_root.arrow_schema,
                                target_top_level_nullable);
  auto nullability = CaptureArrowSchemaNullability(schema_root.arrow_schema);
  transport.target_schema_field_depths = std::move(nullability.field_depths);
  transport.target_schema_field_nullable =
      std::move(nullability.field_nullable);
  transport.schema_fingerprint = SchemaFingerprint(
      column_names, column_types, transport.target_schema_field_depths,
      transport.target_schema_field_nullable, data_storage_version);
  transport.input_names = column_names;
  transport.input_types = column_types;
  transport.data_storage_version = data_storage_version;
}

void LanceDistributedWriteProvider::Impl::Initialize() {
  if (selected) {
    return;
  }
  if (!context) {
    throw InternalException("Distributed Lance write has no planning context");
  }
  ValidateDistributedWriteAutoCommit(*context);
  if (write_kind == LanceDistributedWriteKind::INSERT) {
    InitializeInsert();
  } else {
    InitializeCTAS();
  }
  ValidateTransport(transport);
  plan.extension_name = "lance";
  plan.operator_name = WriteOperatorName(write_kind);
  plan.worker_bind_data = SerializeTransport(transport);
  selected = true;
}

void LanceDistributedWriteProvider::Impl::ValidateShape() const {
  if (!selected || plan.extension_name != "lance" ||
      plan.operator_name != WriteOperatorName(write_kind) ||
      plan.worker_bind_data.empty() ||
      SerializeTransport(transport) != plan.worker_bind_data) {
    throw InvalidInputException(
        "Distributed Lance write target or worker plan was not initialized");
  }
}

void LanceDistributedWriteProvider::Impl::ValidateCTASAbsent() const {
  if (on_conflict != OnCreateConflict::ERROR_ON_CONFLICT) {
    throw NotImplementedException(
        "Distributed Lance CTAS does not support IF NOT EXISTS or OR REPLACE");
  }
  if (DirectoryTableExists(root, table_name, attached_option_keys,
                           attached_option_values)) {
    throw CatalogException(
        "Lance dataset %s already exists before distributed CTAS preparation",
        transport.dataset_uri);
  }
}

void LanceDistributedWriteProvider::Impl::Validate(
    ClientContext &context_p) const {
  ValidateShape();
  if (&context_p != context) {
    throw InvalidInputException(
        "Distributed Lance coordinator context changed after planning");
  }
  ValidateDistributedWriteAutoCommit(context_p);
  ValidateDirectoryReplayState(context_p, root, transport.dataset_uri,
                               attached_option_keys, attached_option_values,
                               uses_coordinator_storage_secret,
                               distributed_replay_path_restricted);
  if (write_kind == LanceDistributedWriteKind::CTAS) {
    ValidateCTASAbsent();
    return;
  }
  ValidateTargetSnapshot(context_p, transport, transport.generation_id);
}

void LanceDistributedWriteProvider::Impl::PrepareCTAS(
    ClientContext &context_p) const {
  ValidateCTASAbsent();
  string open_path;
  vector<string> option_keys;
  vector<string> option_values;
  ResolveWorkerStorageOptions(context_p, transport.dataset_uri, open_path,
                              option_keys, option_values);
  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                  value_ptrs);

  ArrowSchemaWrapper schema_root;
  memset(&schema_root.arrow_schema, 0, sizeof(schema_root.arrow_schema));
  auto properties = context_p.GetClientProperties();
  ArrowConverter::ToArrowSchema(&schema_root.arrow_schema, column_types,
                                column_names, properties);
  ApplyTargetSchemaNullability(schema_root.arrow_schema,
                               transport.target_schema_field_depths,
                               transport.target_schema_field_nullable);
  auto *writer = lance_open_uncommitted_writer_with_storage_options(
      open_path.c_str(), "create", key_ptrs.empty() ? nullptr : key_ptrs.data(),
      value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
      LANCE_DEFAULT_MAX_ROWS_PER_FILE, LANCE_DEFAULT_MAX_ROWS_PER_GROUP,
      LANCE_DEFAULT_MAX_BYTES_PER_FILE, data_storage_version.c_str(),
      LanceGetSessionHandle(context_p), &schema_root.arrow_schema);
  if (!writer) {
    throw IOException(
        "Failed to prepare empty distributed Lance CTAS writer" +
        LanceVaneFormatErrorSuffix(
            transport.dataset_uri,
            LanceVanePathRequiresRedaction(context_p, transport.dataset_uri)));
  }
  void *transaction = nullptr;
  const auto finish_result =
      lance_writer_finish_uncommitted(writer, &transaction);
  lance_close_writer(writer);
  if (finish_result != 0 || !transaction) {
    throw IOException(
        "Failed to prepare empty distributed Lance CTAS transaction" +
        LanceVaneFormatErrorSuffix(
            transport.dataset_uri,
            LanceVanePathRequiresRedaction(context_p, transport.dataset_uri)));
  }
  const auto commit_result = lance_distributed_commit_empty_create(
      open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
      value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
      LanceGetSessionHandle(context_p), transport.operation_id.c_str(),
      transaction);
  if (commit_result != 0) {
    throw IOException(
        "Failed to commit empty distributed Lance CTAS target; outcome may "
        "require Lance orphan cleanup" +
        LanceVaneFormatErrorSuffix(
            transport.dataset_uri,
            LanceVanePathRequiresRedaction(context_p, transport.dataset_uri)));
  }
  auto snapshot = CaptureTargetSnapshot(context_p, transport.dataset_uri);
  const auto transaction_suffix = "-" + transport.operation_id + ".txn";
  if (snapshot.first != transport.expected_version ||
      snapshot.second.find(transaction_suffix) == string::npos) {
    throw TransactionException(
        "Prepared distributed Lance CTAS target does not match its frozen "
        "operation identity");
  }
  prepared_generation = std::move(snapshot.second);
}

void LanceDistributedWriteProvider::Impl::Prepare(
    ClientContext &context_p) const {
  ValidateShape();
  ValidateDistributedWriteAutoCommit(context_p);
  if (prepare_started) {
    throw InvalidInputException(
        "Distributed Lance write preparation started more than once");
  }
  prepare_started = true;
  if (write_kind == LanceDistributedWriteKind::CTAS) {
    PrepareCTAS(context_p);
  } else {
    ValidateTargetSnapshot(context_p, transport, transport.generation_id);
    prepared_generation = transport.generation_id;
  }
  prepared = true;
}

void LanceDistributedWriteProvider::Impl::BestEffortCleanupTransactions(
    ClientContext &context_p,
    const LanceDecodedDistributedCommit &decoded) const noexcept {
  if (decoded.transactions.empty()) {
    return;
  }
  try {
    string open_path;
    vector<string> option_keys;
    vector<string> option_values;
    ResolveWorkerStorageOptions(context_p, transport.dataset_uri, open_path,
                                option_keys, option_values);
    vector<const char *> key_ptrs;
    vector<const char *> value_ptrs;
    BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                    value_ptrs);
    for (const auto &transaction : decoded.transactions) {
      (void)lance_distributed_cleanup_append_transaction(
          open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
          value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
          transport.operation_id.c_str(),
          reinterpret_cast<const uint8_t *>(transaction.data()),
          transaction.size());
      (void)LanceConsumeLastError();
    }
  } catch (...) {
  }
}

void LanceDistributedWriteProvider::Impl::CleanupAttemptManifests(
    ClientContext &context_p,
    const vector<string> &retained_task_attempt_ids) const {
  string open_path;
  vector<string> option_keys;
  vector<string> option_values;
  ResolveWorkerStorageOptions(context_p, transport.dataset_uri, open_path,
                              option_keys, option_values);
  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  vector<const char *> retained_task_attempt_ptrs;
  BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                  value_ptrs);
  retained_task_attempt_ptrs.reserve(retained_task_attempt_ids.size());
  for (const auto &task_attempt_id : retained_task_attempt_ids) {
    retained_task_attempt_ptrs.push_back(task_attempt_id.c_str());
  }
  const auto result = lance_distributed_cleanup_attempt_manifests(
      open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
      value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
      transport.operation_id.c_str(),
      retained_task_attempt_ptrs.empty() ? nullptr
                                         : retained_task_attempt_ptrs.data(),
      retained_task_attempt_ptrs.size());
  if (result != 0) {
    throw IOException(
        "Failed to reconcile distributed Lance attempt cleanup manifests" +
        LanceVaneFormatErrorSuffix(
            transport.dataset_uri,
            LanceVanePathRequiresRedaction(context_p, transport.dataset_uri)));
  }
}

void LanceDistributedWriteProvider::Impl::BestEffortCleanupAttemptManifests(
    ClientContext &context_p) const noexcept {
  try {
    CleanupAttemptManifests(context_p, {});
  } catch (...) {
    (void)LanceConsumeLastError();
  }
}

void LanceDistributedWriteProvider::Impl::BestEffortReleaseAttemptManifests(
    ClientContext &context_p,
    const vector<string> &released_task_attempt_ids) const noexcept {
  try {
    string open_path;
    vector<string> option_keys;
    vector<string> option_values;
    ResolveWorkerStorageOptions(context_p, transport.dataset_uri, open_path,
                                option_keys, option_values);
    vector<const char *> key_ptrs;
    vector<const char *> value_ptrs;
    vector<const char *> released_task_attempt_ptrs;
    BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                    value_ptrs);
    released_task_attempt_ptrs.reserve(released_task_attempt_ids.size());
    for (const auto &task_attempt_id : released_task_attempt_ids) {
      released_task_attempt_ptrs.push_back(task_attempt_id.c_str());
    }
    (void)lance_distributed_release_attempt_manifests(
        open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
        value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
        transport.operation_id.c_str(), released_task_attempt_ptrs.data(),
        released_task_attempt_ptrs.size());
    (void)LanceConsumeLastError();
  } catch (...) {
  }
}

idx_t LanceDistributedWriteProvider::Impl::Finalize(
    ClientContext &context_p,
    const vector<DistributedWriteTaskResult> &results) const {
  ValidateShape();
  ValidateDistributedWriteAutoCommit(context_p);
  if (finalize_started) {
    throw InvalidInputException(
        "Distributed Lance coordinator finalized more than once");
  }
  finalize_started = true;
  if (!prepared || prepared_generation.empty()) {
    throw InvalidInputException(
        "Distributed Lance target was not prepared before finalization");
  }

  LanceDecodedDistributedCommit decoded;
  try {
    auto resolved_info =
        distributed::ResolveDistributedExtensionWriteInfo(context_p, plan);
    DecodeCommitResults(resolved_info, transport, results, decoded);
    ValidateTargetSnapshot(context_p, transport, prepared_generation);
    if (decoded.transactions.empty()) {
      if (decoded.row_count != 0 || decoded.byte_count != 0) {
        throw InvalidInputException(
            "Distributed Lance write returned rows without transactions");
      }
      CleanupAttemptManifests(context_p, {});
      return 0;
    }
    if (decoded.row_count == 0) {
      throw InvalidInputException(
          "Distributed Lance write returned transactions without rows");
    }
    // Vane's attempt-finalization barrier quiesces every peer before exposing
    // one selected attempt per logical task to coordinator finalization.
    // Reconcile every durable worker manifest now: selected attempts retain
    // cleanup ownership through the commit, while successful retry/speculation
    // losers are deleted before catalog mutation.
    CleanupAttemptManifests(context_p, decoded.manifest_task_attempt_ids);
  } catch (...) {
    BestEffortCleanupTransactions(context_p, decoded);
    BestEffortCleanupAttemptManifests(context_p);
    throw;
  }

  string open_path;
  vector<string> option_keys;
  vector<string> option_values;
  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  vector<const uint8_t *> transaction_ptrs;
  vector<size_t> transaction_lengths;
  try {
    ResolveWorkerStorageOptions(context_p, transport.dataset_uri, open_path,
                                option_keys, option_values);
    BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                    value_ptrs);
    transaction_ptrs.reserve(decoded.transactions.size());
    transaction_lengths.reserve(decoded.transactions.size());
    for (const auto &transaction : decoded.transactions) {
      transaction_ptrs.push_back(
          reinterpret_cast<const uint8_t *>(transaction.data()));
      transaction_lengths.push_back(transaction.size());
    }
  } catch (...) {
    BestEffortCleanupTransactions(context_p, decoded);
    BestEffortCleanupAttemptManifests(context_p);
    throw;
  }

  uint8_t commit_started = 0;
  const auto commit_result = lance_distributed_commit_append_transactions(
      open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
      value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
      LanceGetSessionHandle(context_p), transport.expected_version,
      prepared_generation.c_str(), transport.operation_id.c_str(),
      transaction_ptrs.data(), transaction_lengths.data(),
      transaction_ptrs.size(), &commit_started);
  if (commit_result != 0) {
    const auto error_suffix = LanceVaneFormatErrorSuffix(
        transport.dataset_uri,
        LanceVanePathRequiresRedaction(context_p, transport.dataset_uri));
    if (commit_started == 0) {
      BestEffortCleanupTransactions(context_p, decoded);
      BestEffortCleanupAttemptManifests(context_p);
    }
    if (table) {
      LanceInvalidateDatasetCacheForTable(context_p, *table);
    } else {
      LanceInvalidateDatasetCacheForPath(context_p, transport.dataset_uri);
    }
    const auto *message =
        commit_started == 0
            ? "Distributed Lance coordinator commit failed before commit "
              "execution"
            : "Distributed Lance coordinator commit outcome is unknown";
    throw IOException(string(message) + error_suffix);
  }
  if (table) {
    LanceInvalidateDatasetCacheForTable(context_p, *table);
  } else {
    LanceInvalidateDatasetCacheForPath(context_p, transport.dataset_uri);
  }
  // Commit transferred the selected artifacts into a durable Lance version.
  // Release only their temporary ownership records: a concurrent overwrite
  // may already have made the committed files historical, but time travel must
  // continue to retain them.
  BestEffortReleaseAttemptManifests(context_p,
                                    decoded.manifest_task_attempt_ids);
  return decoded.row_count;
}

void LanceDistributedWriteProvider::Impl::Abort(
    ClientContext &context_p,
    const vector<DistributedWriteTaskResult> &results) const {
  ValidateShape();
  if (finalize_started) {
    throw InvalidInputException(
        "Distributed Lance write cannot abort after finalization started");
  }
  LanceDecodedDistributedCommit decoded;
  try {
    if (!results.empty()) {
      auto resolved_info =
          distributed::ResolveDistributedExtensionWriteInfo(context_p, plan);
      DecodeCommitResults(resolved_info, transport, results, decoded);
    }
  } catch (...) {
    BestEffortCleanupTransactions(context_p, decoded);
    BestEffortCleanupAttemptManifests(context_p);
    throw;
  }
  BestEffortCleanupTransactions(context_p, decoded);
  BestEffortCleanupAttemptManifests(context_p);
  // A prepared CTAS target is deliberately retained. Lance has no
  // generation-conditional table deletion primitive, so a check followed by
  // recursive deletion could race with another client committing a live
  // version. The caller must explicitly drop the retained empty target before
  // retrying CTAS.
}

LanceDistributedWriteProvider::LanceDistributedWriteProvider(
    unique_ptr<Impl> impl_p)
    : impl(std::move(impl_p)) {}

LanceDistributedWriteProvider::~LanceDistributedWriteProvider() = default;

optional_ptr<distributed::ExtensionWriteTaskProvider>
LanceDistributedWriteProvider::Select() {
  return this;
}

bool LanceDistributedWriteProvider::DistributedPlanSelected() const {
  return impl->selected;
}

const distributed::DistributedExtensionWritePlan &
LanceDistributedWriteProvider::WritePlan() const {
  impl->Initialize();
  impl->ValidateShape();
  return impl->plan;
}

void LanceDistributedWriteProvider::ValidateDistributedWrite(
    ClientContext &context) const {
  impl->Validate(context);
}

void LanceDistributedWriteProvider::PrepareDistributedWrite(
    ClientContext &context) const {
  impl->Prepare(context);
}

idx_t LanceDistributedWriteProvider::FinalizeDistributedWrite(
    ClientContext &context,
    const vector<DistributedWriteTaskResult> &results) const {
  return impl->Finalize(context, results);
}

void LanceDistributedWriteProvider::AbortDistributedWrite(
    ClientContext &context,
    const vector<DistributedWriteTaskResult> &selected_results) const {
  impl->Abort(context, selected_results);
}

unique_ptr<LanceDistributedWriteProvider> CreateLanceDistributedInsertProvider(
    ClientContext &context, LanceTableEntry &table,
    const vector<string> &column_names, const vector<LogicalType> &column_types,
    const vector<LogicalType> &input_types) {
  auto impl = make_uniq<LanceDistributedWriteProvider::Impl>(context);
  impl->table = table;
  impl->write_kind = LanceDistributedWriteKind::INSERT;
  impl->column_names = column_names;
  impl->column_types = column_types;
  impl->input_types = input_types;
  return unique_ptr<LanceDistributedWriteProvider>(
      new LanceDistributedWriteProvider(std::move(impl)));
}

namespace {

class PhysicalLanceDistributedCreateTableAs final : public PhysicalCopyToFile {
public:
  static constexpr const PhysicalOperatorType TYPE =
      PhysicalOperatorType::EXTENSION;

  PhysicalLanceDistributedCreateTableAs(
      PhysicalPlan &physical_plan, vector<LogicalType> types,
      CopyFunction function, unique_ptr<FunctionData> bind_data,
      idx_t estimated_cardinality,
      unique_ptr<LanceDistributedWriteProvider> distributed_write_p)
      : PhysicalCopyToFile(physical_plan, std::move(types), std::move(function),
                           std::move(bind_data), estimated_cardinality),
        distributed_write(std::move(distributed_write_p)) {
    type = TYPE;
  }

  string GetName() const override { return "LanceCreateTableAs"; }

  optional_ptr<distributed::ExtensionWriteTaskProvider>
  GetExtensionWriteTaskProvider() override {
    if (!distributed_write) {
      throw InternalException(
          "Distributed Lance CTAS has no write-task provider");
    }
    if (children.size() != 1) {
      throw InvalidInputException(
          "Distributed Lance CTAS requires exactly one physical child");
    }
    return distributed_write->Select();
  }

  void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override {
    if (distributed_write && distributed_write->DistributedPlanSelected()) {
      throw InvalidInputException(
          "A distributed Lance CTAS worker plan cannot execute as a native "
          "coordinator operator");
    }
    PhysicalCopyToFile::BuildPipelines(current, meta_pipeline);
  }

private:
  unique_ptr<LanceDistributedWriteProvider> distributed_write;
};

} // namespace

PhysicalOperator &PlanLanceDistributedCreateTableAs(
    ClientContext &context, PhysicalPlanGenerator &planner,
    LogicalCreateTable &op, PhysicalOperator &plan, const string &root,
    const vector<string> &attached_option_keys,
    const vector<string> &attached_option_values,
    bool uses_coordinator_storage_secret,
    bool distributed_replay_path_restricted,
    const string &data_storage_version) {
  auto &create_info = op.info->Base();
  if (!IsSafeDatasetTableName(create_info.table)) {
    throw InvalidInputException("Unsafe Lance dataset name for CREATE TABLE: " +
                                create_info.table);
  }
  if (root.empty()) {
    throw InternalException("Lance directory namespace root is empty");
  }

  const auto dataset_path =
      JoinNamespacePath(root, create_info.table + ".lance");
  const auto exists = DirectoryTableExists(
      root, create_info.table, attached_option_keys, attached_option_values);
  if (create_info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT &&
      exists) {
    return planner.Make<PhysicalEmptyResult>(op.types,
                                             op.estimated_cardinality);
  }
  if (create_info.on_conflict == OnCreateConflict::ERROR_ON_CONFLICT &&
      exists) {
    throw IOException("Lance dataset already exists: " + dataset_path);
  }

  CopyInfo copy_info;
  copy_info.is_from = false;
  copy_info.format = "lance";
  copy_info.file_path = dataset_path;
  copy_info.options["mode"] = {Value(CreateMode(create_info.on_conflict))};
  if (!data_storage_version.empty()) {
    copy_info.options["data_storage_version"] = {Value(data_storage_version)};
  }

  auto &system_catalog = Catalog::GetSystemCatalog(context);
  auto entry = system_catalog.GetEntry(
      context, CatalogType::COPY_FUNCTION_ENTRY, DEFAULT_SCHEMA, "lance",
      OnEntryNotFound::THROW_EXCEPTION);
  auto copy_function = entry->Cast<CopyFunctionCatalogEntry>().function;
  if (!copy_function.copy_to_bind) {
    throw NotImplementedException(
        "COPY TO is not supported for FORMAT \"lance\"");
  }

  auto names = create_info.columns.GetColumnNames();
  auto types = create_info.columns.GetColumnTypes();
  CopyFunctionBindInput bind_input(copy_info);
  auto bind_data =
      copy_function.copy_to_bind(context, bind_input, names, types);

  const auto preserve_insertion_order =
      PhysicalPlanGenerator::PreserveInsertionOrder(context, plan);
  const auto supports_batch_index =
      PhysicalPlanGenerator::UseBatchIndex(context, plan);
  auto execution_mode = CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
  if (copy_function.execution_mode) {
    execution_mode = copy_function.execution_mode(preserve_insertion_order,
                                                  supports_batch_index);
  }
  if (execution_mode == CopyFunctionExecutionMode::BATCH_COPY_TO_FILE) {
    throw NotImplementedException(
        "Distributed Lance CTAS cannot wrap a batch COPY TO operator");
  }

  auto impl = make_uniq<LanceDistributedWriteProvider::Impl>(context);
  impl->write_kind = LanceDistributedWriteKind::CTAS;
  impl->column_names = names;
  impl->column_types = types;
  impl->input_types = plan.types;
  impl->target_top_level_nullable =
      TargetTopLevelNullability(create_info.columns, create_info.constraints);
  impl->root = root;
  impl->attached_option_keys = attached_option_keys;
  impl->attached_option_values = attached_option_values;
  impl->uses_coordinator_storage_secret = uses_coordinator_storage_secret;
  impl->distributed_replay_path_restricted = distributed_replay_path_restricted;
  impl->on_conflict = create_info.on_conflict;
  impl->catalog_name = op.schema.catalog.GetName();
  impl->schema_name = op.schema.name;
  impl->table_name = create_info.table;
  impl->data_storage_version = data_storage_version;
  auto provider = unique_ptr<LanceDistributedWriteProvider>(
      new LanceDistributedWriteProvider(std::move(impl)));

  auto &copy_operator = planner.Make<PhysicalLanceDistributedCreateTableAs>(
      op.types, std::move(copy_function), std::move(bind_data),
      op.estimated_cardinality, std::move(provider));
  auto &copy =
      static_cast<PhysicalLanceDistributedCreateTableAs &>(copy_operator);
  copy.file_path = dataset_path;
  copy.use_tmp_file = false;
  copy.filename_pattern = FilenamePattern();
  copy.file_extension = "";
  copy.overwrite_mode = CopyOverwriteMode::COPY_ERROR_ON_CONFLICT;
  copy.return_type = CopyFunctionReturnType::CHANGED_ROWS;
  copy.per_thread_output = false;
  copy.file_size_bytes = optional_idx();
  copy.rotate = false;
  copy.write_empty_file = true;
  copy.partition_output = false;
  copy.write_partition_columns = false;
  copy.hive_file_pattern = false;
  copy.partition_columns.clear();
  copy.names = std::move(names);
  copy.expected_types = std::move(types);
  copy.parallel =
      execution_mode == CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
  copy.children.push_back(plan);
  return copy_operator;
}

namespace {

struct LanceEncodedTransactionDeleter {
  void operator()(void *transaction) const {
    if (transaction) {
      lance_free_distributed_transaction(transaction);
    }
  }
};

static void
DecodeCommitResults(const DistributedExtensionWriteInfo &info,
                    const LanceDistributedWriteTransport &transport,
                    const vector<DistributedWriteTaskResult> &results,
                    LanceDecodedDistributedCommit &decoded) {
  ValidateResolvedInfo(info, transport);
  unordered_set<string> task_attempt_ids;
  unordered_set<string> logical_task_ids;
  unordered_set<string> fragment_ids;
  unordered_set<string> transaction_payloads;
  unordered_set<string> artifact_paths;
  const auto expected_serialized_transport = SerializeTransport(transport);
  string query_id;
  string duplicate_logical_task_id;

  for (const auto &result : results) {
    result.Validate();
    if (result.capability != info.capability ||
        result.fragment_codec != info.fragment_codec) {
      throw InvalidInputException(
          "Distributed Lance task result does not match its coordinator "
          "contract");
    }
    if (query_id.empty()) {
      query_id = result.query_id;
    } else if (query_id != result.query_id) {
      throw InvalidInputException(
          "Distributed Lance selected results have inconsistent query "
          "identities");
    }
    if (!task_attempt_ids.insert(result.task_attempt_id).second) {
      throw InvalidInputException(
          "Distributed Lance selected task attempt '%s' more than once",
          result.task_attempt_id);
    }
    auto logical_task_id =
        VaneLogicalTaskIdentity(result.query_id, result.task_attempt_id);
    if (!logical_task_ids.insert(logical_task_id).second &&
        duplicate_logical_task_id.empty()) {
      duplicate_logical_task_id = std::move(logical_task_id);
    }
    if (result.fragments.empty()) {
      if (result.RowCount() != 0 || result.ByteCount() != 0) {
        throw InvalidInputException(
            "Empty distributed Lance task result has non-zero counts");
      }
      continue;
    }
    if (result.fragments.size() != 1) {
      throw InvalidInputException(
          "Distributed Lance task result must contain at most one commit "
          "fragment");
    }

    const auto &fragment = result.fragments[0];
    const auto expected_fragment_id =
        transport.operation_id + ":" + result.task_attempt_id;
    if (fragment.fragment_id != expected_fragment_id ||
        !fragment_ids.insert(fragment.fragment_id).second ||
        fragment.row_count == 0 || fragment.row_count != result.RowCount() ||
        fragment.byte_count != result.ByteCount()) {
      throw InvalidInputException(
          "Distributed Lance task result has invalid fragment metadata");
    }
    auto envelope = DeserializeCommitEnvelope(fragment.payload);
    if (envelope.operation_id != transport.operation_id ||
        envelope.dataset_uri != transport.dataset_uri ||
        envelope.expected_version != transport.expected_version ||
        envelope.generation_id != transport.generation_id ||
        envelope.creation_uuid != transport.creation_uuid ||
        envelope.schema_fingerprint != transport.schema_fingerprint ||
        envelope.query_id != result.query_id ||
        envelope.task_attempt_id != result.task_attempt_id ||
        envelope.serialized_transport != expected_serialized_transport ||
        envelope.row_count != fragment.row_count ||
        envelope.byte_count != fragment.byte_count ||
        envelope.artifact_paths.size() != fragment.artifacts.size()) {
      throw InvalidInputException(
          "Distributed Lance commit fragment does not match its frozen "
          "target or task attempt");
    }

    idx_t envelope_rows = 0;
    idx_t envelope_bytes = 0;
    vector<string> decoded_artifact_paths;
    vector<uint64_t> decoded_artifact_sizes;
    for (idx_t transaction_index = 0;
         transaction_index < envelope.transactions.size();
         transaction_index++) {
      const auto &payload = envelope.transactions[transaction_index];
      if (payload.empty() || !transaction_payloads.insert(payload).second) {
        throw InvalidInputException(
            "Distributed Lance selected a duplicate or empty append "
            "transaction");
      }
      const auto transaction_rows =
          envelope.transaction_row_counts[transaction_index];
      const auto transaction_bytes =
          envelope.transaction_byte_counts[transaction_index];
      if (transaction_rows == 0 ||
          transaction_rows > NumericLimits<idx_t>::Maximum() ||
          transaction_bytes > NumericLimits<idx_t>::Maximum()) {
        throw InvalidInputException(
            "Distributed Lance append transaction has invalid counts");
      }
      envelope_rows =
          CheckedAdd(envelope_rows, NumericCast<idx_t>(transaction_rows),
                     "envelope row count");
      envelope_bytes =
          CheckedAdd(envelope_bytes, NumericCast<idx_t>(transaction_bytes),
                     "envelope byte count");

      auto *transaction = lance_distributed_decode_append_transaction(
          reinterpret_cast<const uint8_t *>(payload.data()), payload.size(),
          transport.expected_version, transport.operation_id.c_str(),
          result.query_id.c_str(), result.task_attempt_id.c_str(),
          transaction_rows);
      if (!transaction) {
        throw InvalidInputException(
            "Failed to validate distributed Lance append transaction" +
            LanceVaneFormatErrorSuffix(
                transport.dataset_uri,
                LanceVanePathIsRemote(transport.dataset_uri)));
      }
      unique_ptr<void, LanceEncodedTransactionDeleter> transaction_owner(
          transaction);
      if (lance_distributed_transaction_byte_count(transaction) !=
          transaction_bytes) {
        throw InvalidInputException(
            "Distributed Lance append transaction byte count does not match "
            "its envelope");
      }
      const auto artifact_count =
          lance_distributed_transaction_artifact_count(transaction);
      for (idx_t artifact_index = 0; artifact_index < artifact_count;
           artifact_index++) {
        const auto *path = lance_distributed_transaction_artifact_path(
            transaction, artifact_index);
        if (!path || string(path).empty() ||
            !artifact_paths.insert(path).second) {
          throw InvalidInputException(
              "Distributed Lance selected an empty or duplicate data-file "
              "artifact");
        }
        decoded_artifact_paths.emplace_back(path);
        decoded_artifact_sizes.push_back(
            lance_distributed_transaction_artifact_size(transaction,
                                                        artifact_index));
      }
      decoded.transactions.push_back(payload);
    }
    if (envelope_rows != envelope.row_count ||
        envelope_bytes != envelope.byte_count ||
        decoded_artifact_paths != envelope.artifact_paths ||
        decoded_artifact_sizes != envelope.artifact_sizes) {
      throw InvalidInputException(
          "Distributed Lance commit envelope counts or artifacts do not "
          "match its transactions");
    }
    for (idx_t artifact_index = 0; artifact_index < fragment.artifacts.size();
         artifact_index++) {
      const auto &artifact = fragment.artifacts[artifact_index];
      if (artifact.artifact_id != "data:" + to_string(artifact_index) ||
          artifact.uri != envelope.artifact_paths[artifact_index] ||
          artifact.codec !=
              DistributedPayloadCodec{
                  LANCE_DISTRIBUTED_DATA_ARTIFACT_CODEC,
                  LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION} ||
          !artifact.payload.empty()) {
        throw InvalidInputException(
            "Distributed Lance data-file artifact does not match its commit "
            "envelope");
      }
    }
    decoded.row_count =
        CheckedAdd(decoded.row_count, envelope.row_count, "selected row count");
    decoded.byte_count = CheckedAdd(decoded.byte_count, envelope.byte_count,
                                    "selected byte count");
    decoded.manifest_task_attempt_ids.push_back(result.task_attempt_id);
  }
  if (!duplicate_logical_task_id.empty()) {
    throw InvalidInputException(
        "Distributed Lance write selected multiple attempts for Vane logical "
        "task '%s'",
        duplicate_logical_task_id);
  }
}

} // namespace

namespace {

struct LanceWorkerTransaction {
  string payload;
  vector<string> artifact_paths;
  vector<uint64_t> artifact_sizes;
  idx_t row_count = 0;
  idx_t byte_count = 0;
};

static void BestEffortCleanupTransactions(
    const LanceDistributedWriteTransport &transport, const string &open_path,
    const vector<string> &option_keys, const vector<string> &option_values,
    const vector<LanceWorkerTransaction> &transactions) noexcept {
  try {
    vector<const char *> key_ptrs;
    vector<const char *> value_ptrs;
    BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                    value_ptrs);
    for (const auto &transaction : transactions) {
      (void)lance_distributed_cleanup_append_transaction(
          open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
          value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
          transport.operation_id.c_str(),
          reinterpret_cast<const uint8_t *>(transaction.payload.data()),
          transaction.payload.size());
      (void)LanceConsumeLastError();
    }
  } catch (...) {
  }
}

static void BestEffortCleanupTransactionHandle(
    const string &open_path, const vector<string> &option_keys,
    const vector<string> &option_values, void *transaction) noexcept {
  auto *transaction_to_cleanup = transaction;
  if (!transaction_to_cleanup) {
    return;
  }
  try {
    vector<const char *> key_ptrs;
    vector<const char *> value_ptrs;
    BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                    value_ptrs);
    (void)lance_distributed_cleanup_append_transaction_handle(
        open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
        value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
        transaction_to_cleanup);
    // The cleanup FFI consumes the transaction on every return path.
    transaction_to_cleanup = nullptr;
    (void)LanceConsumeLastError();
  } catch (...) {
  }
  if (transaction_to_cleanup) {
    lance_free_transaction(transaction_to_cleanup);
  }
}

class LanceUncommittedTransactionGuard {
public:
  LanceUncommittedTransactionGuard(void *transaction_p,
                                   const string &open_path_p,
                                   const vector<string> &option_keys_p,
                                   const vector<string> &option_values_p)
      : transaction(transaction_p), open_path(open_path_p),
        option_keys(option_keys_p), option_values(option_values_p) {}

  LanceUncommittedTransactionGuard(const LanceUncommittedTransactionGuard &) =
      delete;
  LanceUncommittedTransactionGuard &
  operator=(const LanceUncommittedTransactionGuard &) = delete;

  ~LanceUncommittedTransactionGuard() {
    BestEffortCleanupTransactionHandle(open_path, option_keys, option_values,
                                       transaction);
  }

  void MarkRegistered() noexcept {
    lance_free_transaction(transaction);
    transaction = nullptr;
  }

private:
  void *transaction;
  const string &open_path;
  const vector<string> &option_keys;
  const vector<string> &option_values;
};

class LanceDistributedWriteGlobalState final
    : public DistributedWriteGlobalState {
public:
  ~LanceDistributedWriteGlobalState() override {
    BestEffortCleanupTransactions(transport, open_path, option_keys,
                                  option_values, transactions);
  }

  LanceDistributedWriteTransport transport;
  string open_path;
  vector<string> option_keys;
  vector<string> option_values;
  string query_id;
  string task_attempt_id;
  ArrowSchemaWrapper schema_root;
  mutex lock;
  vector<LanceWorkerTransaction> transactions;
  idx_t row_count = 0;
  idx_t byte_count = 0;
  bool finalized = false;
};

class LanceDistributedWriteLocalState final
    : public DistributedWriteLocalState {
public:
  ~LanceDistributedWriteLocalState() override {
    auto *writer_to_abort = writer;
    writer = nullptr;
    if (!writer_to_abort) {
      return;
    }
    try {
      vector<const char *> key_ptrs;
      vector<const char *> value_ptrs;
      BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                      value_ptrs);
      (void)lance_distributed_abort_uncommitted_writer(
          writer_to_abort, open_path.c_str(),
          key_ptrs.empty() ? nullptr : key_ptrs.data(),
          value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
          expected_version, operation_id.c_str(), query_id.c_str(),
          task_attempt_id.c_str());
      // The abort FFI consumes the writer even when finalization or cleanup
      // fails, so it must not be closed a second time.
      writer_to_abort = nullptr;
      (void)LanceConsumeLastError();
    } catch (...) {
    }
    if (writer_to_abort) {
      lance_close_writer(writer_to_abort);
    }
  }

  void *writer = nullptr;
  idx_t row_count = 0;
  string open_path;
  vector<string> option_keys;
  vector<string> option_values;
  uint64_t expected_version = 0;
  string operation_id;
  string query_id;
  string task_attempt_id;
};

static void ValidateWorkerTask(const LanceDistributedWriteGlobalState &global,
                               const DistributedWriteTaskContext &task) {
  task.Validate();
  (void)VaneLogicalTaskIdentity(task.query_id, task.task_attempt_id);
  if (global.query_id != task.query_id ||
      global.task_attempt_id != task.task_attempt_id) {
    throw InvalidInputException(
        "Distributed Lance worker task identity changed during execution");
  }
}

static unique_ptr<DistributedWriteGlobalState>
LanceDistributedWriteInitializeGlobal(ClientContext &context,
                                      const DistributedExtensionWriteInfo &info,
                                      const DistributedWriteTaskContext &task) {
  task.Validate();
  auto result = make_uniq<LanceDistributedWriteGlobalState>();
  result->transport = DeserializeTransport(info.worker_bind_data);
  ValidateResolvedInfo(info, result->transport);
  if (info.Name() != WriteOperatorName(result->transport.write_kind)) {
    throw InvalidInputException(
        "Distributed Lance write kind does not match its worker operator");
  }
  (void)VaneLogicalTaskIdentity(task.query_id, task.task_attempt_id);
  ResolveWorkerStorageOptions(context, result->transport.dataset_uri,
                              result->open_path, result->option_keys,
                              result->option_values);
  result->query_id = task.query_id;
  result->task_attempt_id = task.task_attempt_id;
  memset(&result->schema_root.arrow_schema, 0,
         sizeof(result->schema_root.arrow_schema));
  auto properties = context.GetClientProperties();
  ArrowConverter::ToArrowSchema(&result->schema_root.arrow_schema,
                                result->transport.input_types,
                                result->transport.input_names, properties);
  // DuckDB LogicalType does not encode field nullability. Restore the complete
  // coordinator-bound Arrow field tree after reconstructing the worker schema
  // so Rust receives the frozen target contract instead of synthetic flags.
  ApplyTargetSchemaNullability(result->schema_root.arrow_schema,
                               result->transport.target_schema_field_depths,
                               result->transport.target_schema_field_nullable);
  return std::move(result);
}

static unique_ptr<DistributedWriteLocalState>
LanceDistributedWriteInitializeLocal(
    ExecutionContext &, const DistributedExtensionWriteInfo &,
    const DistributedWriteTaskContext &task,
    DistributedWriteGlobalState &global_state) {
  auto &global = global_state.Cast<LanceDistributedWriteGlobalState>();
  ValidateWorkerTask(global, task);
  auto result = make_uniq<LanceDistributedWriteLocalState>();
  result->open_path = global.open_path;
  result->option_keys = global.option_keys;
  result->option_values = global.option_values;
  result->expected_version = global.transport.expected_version;
  result->operation_id = global.transport.operation_id;
  result->query_id = task.query_id;
  result->task_attempt_id = task.task_attempt_id;
  return std::move(result);
}

static void OpenWorkerWriter(ClientContext &context,
                             LanceDistributedWriteGlobalState &global,
                             LanceDistributedWriteLocalState &local) {
  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  BuildStorageOptionPointerArrays(global.option_keys, global.option_values,
                                  key_ptrs, value_ptrs);
  const auto &transport = global.transport;
  const char *generation = transport.generation_id.empty()
                               ? nullptr
                               : transport.generation_id.c_str();
  const char *creation_uuid = transport.creation_uuid.empty()
                                  ? nullptr
                                  : transport.creation_uuid.c_str();
  local.writer = lance_open_distributed_uncommitted_writer_with_storage_options(
      global.open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
      value_ptrs.empty() ? nullptr : value_ptrs.data(),
      global.option_keys.size(), transport.expected_version, generation,
      creation_uuid, transport.operation_id.c_str(), global.query_id.c_str(),
      global.task_attempt_id.c_str(), LANCE_DEFAULT_MAX_ROWS_PER_FILE,
      LANCE_DEFAULT_MAX_ROWS_PER_GROUP, LANCE_DEFAULT_MAX_BYTES_PER_FILE,
      LanceGetSessionHandle(context), &global.schema_root.arrow_schema);
  if (!local.writer) {
    throw IOException(
        "Failed to open distributed Lance worker writer" +
        LanceVaneFormatErrorSuffix(
            transport.dataset_uri,
            LanceVanePathRequiresRedaction(context, transport.dataset_uri)));
  }
}

static void LanceDistributedWriteSink(ExecutionContext &context,
                                      const DistributedExtensionWriteInfo &,
                                      const DistributedWriteTaskContext &task,
                                      DistributedWriteGlobalState &global_state,
                                      DistributedWriteLocalState &local_state,
                                      DataChunk &input) {
  auto &global = global_state.Cast<LanceDistributedWriteGlobalState>();
  auto &local = local_state.Cast<LanceDistributedWriteLocalState>();
  ValidateWorkerTask(global, task);
  if (input.GetTypes() != global.transport.input_types) {
    throw InvalidInputException(
        "Distributed Lance worker input schema does not match its frozen "
        "target");
  }
  if (input.size() == 0) {
    return;
  }
  // Top-level DuckDB vectors can be checked directly here. The Rust writer
  // validates nested Arrow values against the restored frozen schema after
  // conversion and before buffering a worker batch.
  idx_t column_index = 0;
  for (idx_t field_index = 0;
       field_index < global.transport.target_schema_field_depths.size();
       field_index++) {
    if (global.transport.target_schema_field_depths[field_index] != 0) {
      continue;
    }
    if (global.transport.target_schema_field_nullable[field_index] == 0 &&
        VectorOperations::HasNull(input.data[column_index], input.size())) {
      throw ConstraintException("NOT NULL constraint failed: %s.%s",
                                global.transport.table_name,
                                global.transport.input_names[column_index]);
    }
    column_index++;
  }
  if (!local.writer) {
    OpenWorkerWriter(context.client, global, local);
  }

  unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>>
      extension_type_cast;
  auto properties = context.client.GetClientProperties();
  ArrowArray array;
  memset(&array, 0, sizeof(array));
  ArrowConverter::ToArrowArray(input, &array, properties, extension_type_cast);
  const auto result = lance_writer_write_batch(local.writer, &array);
  if (array.release) {
    array.release(&array);
  }
  if (result != 0) {
    throw IOException("Failed to write distributed Lance worker batch" +
                      LanceVaneFormatErrorSuffix(
                          global.transport.dataset_uri,
                          LanceVanePathRequiresRedaction(
                              context.client, global.transport.dataset_uri)));
  }
  local.row_count =
      CheckedAdd(local.row_count, input.size(), "worker row count");
}

static void
LanceDistributedWriteCombine(ExecutionContext &context,
                             const DistributedExtensionWriteInfo &,
                             const DistributedWriteTaskContext &task,
                             DistributedWriteGlobalState &global_state,
                             DistributedWriteLocalState &local_state) {
  auto &global = global_state.Cast<LanceDistributedWriteGlobalState>();
  auto &local = local_state.Cast<LanceDistributedWriteLocalState>();
  ValidateWorkerTask(global, task);
  if (!local.writer) {
    if (local.row_count != 0) {
      throw InternalException(
          "Distributed Lance worker lost its writer for non-empty input");
    }
    return;
  }

  void *transaction = nullptr;
  const auto finish_result =
      lance_writer_finish_uncommitted(local.writer, &transaction);
  // Lance's execute_uncommitted_stream failure path drops the in-progress
  // writer and deletes every completed fragment before returning an error.
  // WriteParams::skip_auto_cleanup affects only post-commit version cleanup,
  // not this write-failure cleanup, so the wrapper handle can now be closed.
  lance_close_writer(local.writer);
  local.writer = nullptr;
  if (finish_result != 0 || !transaction) {
    throw IOException(
        "Failed to finalize distributed Lance worker transaction" +
        LanceVaneFormatErrorSuffix(
            global.transport.dataset_uri,
            LanceVanePathRequiresRedaction(context.client,
                                           global.transport.dataset_uri)));
  }
  LanceUncommittedTransactionGuard transaction_guard(
      transaction, global.open_path, global.option_keys, global.option_values);

  auto *encoded = lance_distributed_encode_append_transaction(
      transaction, global.transport.expected_version,
      global.transport.operation_id.c_str(), task.query_id.c_str(),
      task.task_attempt_id.c_str(), local.row_count);
  if (!encoded) {
    throw IOException("Failed to encode distributed Lance worker transaction" +
                      LanceVaneFormatErrorSuffix(
                          global.transport.dataset_uri,
                          LanceVanePathRequiresRedaction(
                              context.client, global.transport.dataset_uri)));
  }
  unique_ptr<void, LanceEncodedTransactionDeleter> encoded_owner(encoded);
  size_t payload_size = 0;
  const auto *payload =
      lance_distributed_transaction_bytes(encoded, &payload_size);
  if (!payload || payload_size == 0) {
    throw IOException("Distributed Lance worker transaction payload is empty" +
                      LanceFormatErrorSuffix());
  }

  LanceWorkerTransaction worker_transaction;
  worker_transaction.payload.assign(reinterpret_cast<const char *>(payload),
                                    payload_size);
  worker_transaction.row_count = local.row_count;
  worker_transaction.byte_count =
      NumericCast<idx_t>(lance_distributed_transaction_byte_count(encoded));
  const auto artifact_count =
      lance_distributed_transaction_artifact_count(encoded);
  worker_transaction.artifact_paths.reserve(artifact_count);
  worker_transaction.artifact_sizes.reserve(artifact_count);
  for (idx_t index = 0; index < artifact_count; index++) {
    const auto *artifact_path =
        lance_distributed_transaction_artifact_path(encoded, index);
    if (!artifact_path || string(artifact_path).empty()) {
      throw IOException(
          "Distributed Lance worker transaction has an empty artifact path");
    }
    worker_transaction.artifact_paths.emplace_back(artifact_path);
    worker_transaction.artifact_sizes.push_back(
        lance_distributed_transaction_artifact_size(encoded, index));
  }
  if (worker_transaction.artifact_paths.empty()) {
    throw IOException(
        "Distributed Lance worker transaction has no data artifacts");
  }

  {
    lock_guard<mutex> guard(global.lock);
    const auto row_count = CheckedAdd(
        global.row_count, worker_transaction.row_count, "task row count");
    const auto byte_count = CheckedAdd(
        global.byte_count, worker_transaction.byte_count, "task byte count");
    global.transactions.push_back(std::move(worker_transaction));
    global.row_count = row_count;
    global.byte_count = byte_count;
  }
  transaction_guard.MarkRegistered();
}

static vector<DistributedWriteFragment>
LanceDistributedWriteFinalize(ClientContext &context,
                              const DistributedExtensionWriteInfo &,
                              const DistributedWriteTaskContext &task,
                              DistributedWriteGlobalState &global_state) {
  auto &global = global_state.Cast<LanceDistributedWriteGlobalState>();
  ValidateWorkerTask(global, task);
  if (global.finalized) {
    throw InvalidInputException(
        "Distributed Lance worker finalized more than once");
  }
  global.finalized = true;
  if (global.transactions.empty()) {
    if (global.row_count != 0 || global.byte_count != 0) {
      throw InternalException(
          "Distributed Lance worker lost transactions for non-empty input");
    }
    return {};
  }

  LanceDistributedCommitEnvelope envelope;
  envelope.operation_id = global.transport.operation_id;
  envelope.dataset_uri = global.transport.dataset_uri;
  envelope.expected_version = global.transport.expected_version;
  envelope.generation_id = global.transport.generation_id;
  envelope.creation_uuid = global.transport.creation_uuid;
  envelope.schema_fingerprint = global.transport.schema_fingerprint;
  envelope.query_id = task.query_id;
  envelope.task_attempt_id = task.task_attempt_id;
  envelope.serialized_transport = SerializeTransport(global.transport);
  envelope.row_count = global.row_count;
  envelope.byte_count = global.byte_count;
  for (const auto &transaction : global.transactions) {
    envelope.transactions.push_back(transaction.payload);
    envelope.transaction_row_counts.push_back(transaction.row_count);
    envelope.transaction_byte_counts.push_back(transaction.byte_count);
    envelope.artifact_paths.insert(envelope.artifact_paths.end(),
                                   transaction.artifact_paths.begin(),
                                   transaction.artifact_paths.end());
    envelope.artifact_sizes.insert(envelope.artifact_sizes.end(),
                                   transaction.artifact_sizes.begin(),
                                   transaction.artifact_sizes.end());
  }

  DistributedWriteFragment fragment;
  fragment.fragment_id =
      global.transport.operation_id + ":" + task.task_attempt_id;
  fragment.payload = SerializeCommitEnvelope(envelope);
  fragment.row_count = global.row_count;
  fragment.byte_count = global.byte_count;
  for (idx_t index = 0; index < envelope.artifact_paths.size(); index++) {
    DistributedWriteArtifact artifact;
    artifact.artifact_id = "data:" + to_string(index);
    artifact.uri = envelope.artifact_paths[index];
    artifact.codec = {LANCE_DISTRIBUTED_DATA_ARTIFACT_CODEC,
                      LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION};
    fragment.artifacts.push_back(std::move(artifact));
  }
  vector<DistributedWriteFragment> fragments;
  fragments.push_back(std::move(fragment));

  vector<const char *> key_ptrs;
  vector<const char *> value_ptrs;
  vector<const uint8_t *> transaction_ptrs;
  vector<size_t> transaction_lengths;
  BuildStorageOptionPointerArrays(global.option_keys, global.option_values,
                                  key_ptrs, value_ptrs);
  transaction_ptrs.reserve(global.transactions.size());
  transaction_lengths.reserve(global.transactions.size());
  for (const auto &transaction : global.transactions) {
    transaction_ptrs.push_back(
        reinterpret_cast<const uint8_t *>(transaction.payload.data()));
    transaction_lengths.push_back(transaction.payload.size());
  }
  const auto publish_result = lance_distributed_publish_attempt_manifest(
      global.open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
      value_ptrs.empty() ? nullptr : value_ptrs.data(),
      global.option_keys.size(), global.transport.expected_version,
      global.transport.operation_id.c_str(), task.query_id.c_str(),
      task.task_attempt_id.c_str(), transaction_ptrs.data(),
      transaction_lengths.data(), transaction_ptrs.size());
  if (publish_result != 0) {
    throw IOException(
        "Failed to publish distributed Lance attempt cleanup manifest" +
        LanceVaneFormatErrorSuffix(global.transport.dataset_uri,
                                   LanceVanePathRequiresRedaction(
                                       context, global.transport.dataset_uri)));
  }

  // Ownership has moved from worker memory to a durable attempt manifest.
  // The coordinator removes retry/speculation losers after Vane selects the
  // winning attempts, and retains selected ownership until commit succeeds.
  global.transactions.clear();
  return fragments;
}

static DistributedExtensionWriteCallbacks LanceDistributedWriteCallbacks() {
  DistributedExtensionWriteCallbacks callbacks;
  callbacks.initialize_global = LanceDistributedWriteInitializeGlobal;
  callbacks.initialize_local = LanceDistributedWriteInitializeLocal;
  callbacks.sink = LanceDistributedWriteSink;
  callbacks.combine = LanceDistributedWriteCombine;
  callbacks.finalize = LanceDistributedWriteFinalize;
  return callbacks;
}

} // namespace

void RegisterLanceDistributedWrites(ExtensionLoader &loader) {
  for (const auto *operator_name :
       {LANCE_DISTRIBUTED_INSERT_OPERATOR, LANCE_DISTRIBUTED_CTAS_OPERATOR}) {
    DistributedWriteOperatorExtension extension;
    extension.name = operator_name;
    extension.protocol_version = LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION;
    extension.mode = DistributedWriteMode::CALLBACK;
    extension.fragment_codec = {LANCE_DISTRIBUTED_WRITE_FRAGMENT_CODEC,
                                LANCE_DISTRIBUTED_WRITE_PROTOCOL_VERSION};
    extension.callbacks = LanceDistributedWriteCallbacks();
    DistributedWriteOperatorExtension::Register(loader, std::move(extension));
  }
}

} // namespace duckdb

#endif
