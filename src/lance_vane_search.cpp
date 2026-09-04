#ifdef LANCE_VANE_DISTRIBUTED

#include "lance_vane_search.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "lance_arrow_compat.hpp"
#include "lance_common.hpp"
#include "lance_ffi.hpp"
#include "lance_filter_ir.hpp"
#include "lance_session_state.hpp"
#include "lance_vane_snapshot.hpp"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <functional>

namespace duckdb {

static constexpr idx_t LANCE_VANE_SEARCH_PROTOCOL_VERSION = 1;
static constexpr uint64_t LANCE_VANE_SEARCH_CONTRACT_VERSION = 1;
static constexpr uint64_t LANCE_VANE_FROZEN_SEARCH_SNAPSHOT_VERSION = 1;
static constexpr idx_t LANCE_VANE_SEARCH_SPLIT_CODEC_VERSION = 1;
static constexpr const char *LANCE_VANE_SEARCH_SPLIT_CODEC =
    "lance.search-task";
static constexpr const char *LANCE_VANE_SEARCH_STATE_HASH_DOMAIN =
    "lance.vane.global-search-state.sha256";
static constexpr const char *LANCE_VANE_FINAL_SEARCH_TASK_ID_PREFIX =
    "final-search:";
static constexpr const char *LANCE_VANE_VECTOR_CANDIDATE_TASK_ID_PREFIX =
    "vector-candidates:";
static constexpr const char
    *LANCE_VANE_INDEXED_VECTOR_CANDIDATE_TASK_ID_PREFIX =
        "indexed-vector-candidates:";
static constexpr const char *LANCE_VANE_FTS_CANDIDATE_TASK_ID_PREFIX =
    "fts-candidates:";
static constexpr idx_t LANCE_VANE_VECTOR_CANDIDATE_MIN_FRAGMENTS = 2;
static constexpr uint64_t LANCE_VANE_VECTOR_CANDIDATE_MIN_DISTANCE_VALUES =
    1ULL << 20;
static constexpr uint64_t LANCE_VANE_FTS_CANDIDATE_MIN_ROWS = 4096;
static constexpr idx_t LANCE_VANE_SEARCH_UUID_SIZE = BaseUUID::STRING_SIZE;
static constexpr idx_t LANCE_VANE_SHA256_SIZE = 32;
static constexpr idx_t LANCE_VANE_SEARCH_TASK_BASE_PAYLOAD_SIZE =
    1 + LANCE_VANE_SEARCH_UUID_SIZE + LANCE_VANE_SHA256_SIZE;
static constexpr idx_t LANCE_VANE_VECTOR_CANDIDATE_TASK_PAYLOAD_SIZE =
    LANCE_VANE_SEARCH_TASK_BASE_PAYLOAD_SIZE + sizeof(uint64_t);
static constexpr idx_t LANCE_VANE_INDEX_SEGMENT_UUID_SIZE = 16;
static constexpr idx_t LANCE_VANE_INDEXED_VECTOR_TASK_PAYLOAD_SIZE =
    LANCE_VANE_SEARCH_TASK_BASE_PAYLOAD_SIZE + 1 +
    LANCE_VANE_INDEX_SEGMENT_UUID_SIZE;
static constexpr idx_t LANCE_VANE_FTS_CANDIDATE_TASK_PAYLOAD_SIZE =
    LANCE_VANE_SEARCH_TASK_BASE_PAYLOAD_SIZE +
    LANCE_VANE_INDEX_SEGMENT_UUID_SIZE;

struct LanceVaneSearchBytesDeleter {
  size_t len;

  void operator()(uint8_t *value) const {
    if (value) {
      lance_vane_free_bytes(value, len);
    }
  }
};

struct LanceVanePlanningDatasetDeleter {
  void operator()(void *value) const {
    if (value) {
      lance_close_dataset(value);
    }
  }
};

class LanceVaneCanonicalWriter {
public:
  void U8(uint8_t value) { bytes.push_back(static_cast<char>(value)); }

  void U32(uint32_t value) {
    for (idx_t i = 0; i < sizeof(value); i++) {
      bytes.push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
  }

  void U64(uint64_t value) {
    for (idx_t i = 0; i < sizeof(value); i++) {
      bytes.push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
  }

  void Bool(bool value) { U8(value ? 1 : 0); }

  void String(const string &value) {
    U64(NumericCast<uint64_t>(value.size()));
    bytes.append(value);
  }

  void Float(float value) {
    uint32_t bits = 0;
    static_assert(sizeof(bits) == sizeof(value), "float must be 32 bits");
    std::memcpy(&bits, &value, sizeof(bits));
    U32(bits);
  }

  template <class T, class FUNC>
  void Vector(const vector<T> &values, FUNC &&func) {
    U64(NumericCast<uint64_t>(values.size()));
    for (auto &value : values) {
      func(value);
    }
  }

  const string &Bytes() const { return bytes; }

private:
  string bytes;
};

static bool IsValidSearchUUID(const string &value) {
  hugeint_t parsed;
  return value.size() == LANCE_VANE_SEARCH_UUID_SIZE &&
         BaseUUID::FromString(value, parsed, true);
}

static string LanceVaneSha256(const string &value) {
  string result(LANCE_VANE_SHA256_SIZE, '\0');
  auto rc =
      lance_vane_sha256(reinterpret_cast<const uint8_t *>(value.data()),
                        value.size(), reinterpret_cast<uint8_t *>(&result[0]));
  if (rc != 0) {
    throw IOException("Failed to compute distributed Lance search digest" +
                      LanceFormatErrorSuffix());
  }
  return result;
}

static void
ValidateAndMarkSearchPlanPayloads(LanceVaneGlobalSearchState &state) {
  auto *index_plan = reinterpret_cast<const uint8_t *>(state.index_plan.data());
  auto *vector_column = state.arguments.vector_column.empty()
                            ? nullptr
                            : state.arguments.vector_column.c_str();
  auto *text_column = state.arguments.text_column.empty()
                          ? nullptr
                          : state.arguments.text_column.c_str();
  if (lance_vane_validate_search_index_plan(
          index_plan, state.index_plan.size(), state.dataset_version,
          state.dataset_generation_id.c_str(),
          static_cast<uint8_t>(state.arguments.kind), vector_column,
          text_column, state.arguments.use_index ? 1 : 0) != 0) {
    throw SerializationException(
        "Distributed Lance SearchIndexPlan is malformed" +
        LanceFormatErrorSuffix());
  }
  if (!state.namespace_filter_plan.empty()) {
    auto *namespace_filter =
        reinterpret_cast<const uint8_t *>(state.namespace_filter_plan.data());
    if (lance_vane_validate_namespace_filter_plan(
            namespace_filter, state.namespace_filter_plan.size()) != 0) {
      throw SerializationException(
          "Distributed Lance NamespaceFilterPlan is malformed" +
          LanceFormatErrorSuffix());
    }
  }
  state.search_plan_payloads_validated = true;
}

static void
ValidateAndMarkFrozenSearchSnapshot(LanceVaneGlobalSearchState &state) {
  if (!state.frozen_snapshot) {
    throw SerializationException(
        "Distributed Lance search has no frozen snapshot payload");
  }
  auto &snapshot = *state.frozen_snapshot;
  string validation_error;
  if (!LanceVaneValidateFrozenSnapshot(snapshot.dataset.serialized_manifest,
                                       snapshot.dataset.manifest_sha256,
                                       snapshot.dataset.schema_fingerprint,
                                       validation_error)) {
    throw SerializationException(
        "Distributed Lance search has an invalid frozen manifest: " +
        validation_error);
  }
  if (snapshot.dataset.schema_fingerprint != state.schema_fingerprint) {
    throw SerializationException(
        "Distributed Lance search frozen schema identity does not match");
  }
  if (snapshot.serialized_index_section.size() >
      LANCE_VANE_MAX_SERIALIZED_INDEX_SECTION_BYTES) {
    throw SerializationException(
        "Distributed Lance search index section exceeds the transport limit");
  }
  if (snapshot.index_section_sha256.size() != LANCE_VANE_SHA256_SIZE ||
      LanceVaneSha256(snapshot.serialized_index_section) !=
          snapshot.index_section_sha256) {
    throw SerializationException(
        "Distributed Lance search index section digest does not match its "
        "payload");
  }
  state.frozen_snapshot_payload_validated = true;
}

static void AppendColumnIndex(LanceVaneCanonicalWriter &writer,
                              const ColumnIndex &column) {
  writer.Bool(column.HasPrimaryIndex());
  if (column.HasPrimaryIndex()) {
    writer.U64(column.GetPrimaryIndex());
  } else {
    writer.String(column.GetFieldName());
  }
  writer.Bool(column.HasType());
  if (column.HasType()) {
    writer.String(column.GetType().ToString());
  }
  writer.Bool(column.IsPushdownExtract());
  writer.Vector(column.GetChildIndexes(), [&](const ColumnIndex &child) {
    AppendColumnIndex(writer, child);
  });
}

static string
BuildFilterFingerprint(const vector<string> &names,
                       const vector<ColumnIndex> &column_ids,
                       optional_ptr<const TableFilterSet> filters) {
  LanceVaneCanonicalWriter writer;
  if (!filters) {
    writer.U64(0);
    return LanceVaneSha256(writer.Bytes());
  }
  writer.U64(filters->filters.size());
  for (auto &entry : filters->filters) {
    auto slot = entry.first;
    if (slot >= column_ids.size()) {
      throw InvalidInputException(
          "Distributed Lance search filter references an invalid column");
    }
    auto &column = column_ids[slot];
    if (!column.HasPrimaryIndex() || column.IsVirtualColumn() ||
        column.GetPrimaryIndex() >= names.size()) {
      throw InvalidInputException(
          "Distributed Lance search cannot fingerprint a virtual filter "
          "column");
    }
    writer.U64(slot);
    AppendColumnIndex(writer, column);
    writer.U8(static_cast<uint8_t>(entry.second->filter_type));
    writer.String(entry.second->ToString(names[column.GetPrimaryIndex()]));
  }
  return LanceVaneSha256(writer.Bytes());
}

static string
CanonicalSearchStateBytes(const LanceVaneGlobalSearchState &state) {
  LanceVaneCanonicalWriter writer;
  writer.String(LANCE_VANE_SEARCH_STATE_HASH_DOMAIN);
  writer.U64(state.contract_version);
  writer.U64(LANCE_VANE_SEARCH_PROTOCOL_VERSION);
  writer.Bool(state.valid);
  writer.Bool(state.finalized);
  writer.String(state.qualification_failure);
  writer.U8(static_cast<uint8_t>(state.source_class));
  writer.String(state.physical_uri);
  writer.U64(state.dataset_version);
  writer.String(state.dataset_generation_id);
  writer.String(state.schema_fingerprint);
  writer.String(state.search_node_uuid);
  writer.Bool(state.private_uri_diagnostics);
  writer.U8(static_cast<uint8_t>(state.arguments.kind));
  writer.U8(static_cast<uint8_t>(state.arguments.overload));
  writer.String(state.arguments.vector_column);
  writer.Vector(state.arguments.vector_query,
                [&](float value) { writer.Float(value); });
  writer.String(state.arguments.text_column);
  writer.String(state.arguments.text_query);
  writer.U64(state.arguments.k);
  writer.U64(state.arguments.nprobes);
  writer.U64(state.arguments.refine_factor);
  writer.Bool(state.arguments.prefilter);
  writer.Bool(state.arguments.use_index);
  writer.Bool(state.arguments.explain_verbose);
  writer.Bool(state.arguments.namespace_backed);
  writer.Float(state.arguments.alpha);
  writer.U32(state.arguments.oversample_factor);
  writer.Vector(state.output_names,
                [&](const string &name) { writer.String(name); });
  writer.Vector(state.output_types, [&](const LogicalType &type) {
    writer.String(type.ToString());
  });
  writer.Vector(state.column_ids, [&](const ColumnIndex &column) {
    AppendColumnIndex(writer, column);
  });
  writer.Vector(state.projection_ids,
                [&](idx_t projection) { writer.U64(projection); });
  writer.String(state.final_filter_ir);
  writer.String(state.namespace_filter_plan);
  writer.String(state.filter_fingerprint);
  writer.Bool(state.filter_pushed_down);
  writer.Vector(state.pending_filter_ir_parts,
                [&](const string &part) { writer.String(part); });
  writer.Bool(state.pending_complex_filter_pushdown_failed);
  writer.String(state.index_plan);
  writer.U8(static_cast<uint8_t>(state.execution_variant));
  writer.Vector(state.fragment_ids,
                [&](uint64_t fragment_id) { writer.U64(fragment_id); });
  writer.Vector(state.fragment_row_counts, [&](int64_t row_count) {
    writer.U64(static_cast<uint64_t>(row_count));
  });
  writer.Vector(state.fragment_bytes_on_disk,
                [&](uint64_t bytes) { writer.U64(bytes); });
  writer.Vector(state.indexed_vector_segment_uuids,
                [&](const string &uuid) { writer.String(uuid); });
  writer.Vector(state.indexed_vector_segment_fragment_offsets,
                [&](uint64_t offset) { writer.U64(offset); });
  writer.Vector(state.indexed_vector_segment_fragment_ids,
                [&](uint64_t fragment_id) { writer.U64(fragment_id); });
  writer.Vector(state.indexed_vector_uncovered_fragment_ids,
                [&](uint64_t fragment_id) { writer.U64(fragment_id); });
  writer.Vector(state.fts_segment_uuids,
                [&](const string &uuid) { writer.String(uuid); });
  writer.Vector(state.fts_segment_fragment_offsets,
                [&](uint64_t offset) { writer.U64(offset); });
  writer.Vector(state.fts_segment_fragment_ids,
                [&](uint64_t fragment_id) { writer.U64(fragment_id); });
  writer.Bool(static_cast<bool>(state.frozen_snapshot));
  if (state.frozen_snapshot) {
    writer.String(state.frozen_snapshot->dataset.manifest_sha256);
    writer.String(state.frozen_snapshot->dataset.schema_fingerprint);
    writer.String(state.frozen_snapshot->index_section_sha256);
  }
  return writer.Bytes();
}

static void ValidateSearchArguments(const LanceVaneSearchArguments &arguments) {
  if (arguments.k == 0 || arguments.oversample_factor == 0 ||
      !std::isfinite(arguments.alpha)) {
    throw SerializationException(
        "Distributed Lance search has invalid numeric arguments");
  }
  switch (arguments.kind) {
  case LanceVaneSearchKind::VECTOR:
    if (arguments.vector_column.empty() || arguments.vector_query.empty() ||
        !arguments.text_column.empty() || !arguments.text_query.empty()) {
      throw SerializationException(
          "Distributed Lance vector search has invalid arguments");
    }
    break;
  case LanceVaneSearchKind::FTS:
    if (arguments.text_column.empty() || !arguments.vector_column.empty() ||
        !arguments.vector_query.empty()) {
      throw SerializationException(
          "Distributed Lance FTS has invalid arguments");
    }
    break;
  case LanceVaneSearchKind::HYBRID:
    if (arguments.vector_column.empty() || arguments.vector_query.empty() ||
        arguments.text_column.empty()) {
      throw SerializationException(
          "Distributed Lance hybrid search has invalid arguments");
    }
    break;
  }
  for (auto value : arguments.vector_query) {
    if (!std::isfinite(value)) {
      throw SerializationException(
          "Distributed Lance search contains a non-finite vector value");
    }
  }
}

static bool
SearchKindMatchesOverload(const LanceVaneSearchArguments &arguments) {
  switch (arguments.kind) {
  case LanceVaneSearchKind::VECTOR:
    return arguments.overload == LanceVaneSearchOverload::VECTOR_FLOAT ||
           arguments.overload == LanceVaneSearchOverload::VECTOR_DOUBLE;
  case LanceVaneSearchKind::FTS:
    return arguments.overload == LanceVaneSearchOverload::FTS;
  case LanceVaneSearchKind::HYBRID:
    return arguments.overload == LanceVaneSearchOverload::HYBRID_FLOAT ||
           arguments.overload == LanceVaneSearchOverload::HYBRID_DOUBLE;
  }
  return false;
}

struct LanceVaneSearchTaskAssignment {
  LanceVaneSearchTaskVariant variant = LanceVaneSearchTaskVariant::FINAL_SEARCH;
  string search_node_uuid;
  string state_sha256;
  optional_idx fragment_id;
  string index_segment_uuid;
};

enum class LanceVaneIndexedVectorWorkKind : uint8_t {
  INDEX_SEGMENT = 0,
  FLAT_FRAGMENT = 1
};

static void AppendU64(string &payload, uint64_t value) {
  for (idx_t i = 0; i < sizeof(value); i++) {
    payload.push_back(static_cast<char>((value >> (i * 8)) & 0xff));
  }
}

static uint64_t ReadU64(const string &payload, idx_t offset) {
  uint64_t result = 0;
  for (idx_t i = 0; i < sizeof(result); i++) {
    result |= static_cast<uint64_t>(static_cast<uint8_t>(payload[offset + i]))
              << (i * 8);
  }
  return result;
}

static string
EncodeSearchTaskAssignment(const LanceVaneSearchTaskAssignment &assignment) {
  switch (assignment.variant) {
  case LanceVaneSearchTaskVariant::FINAL_SEARCH:
  case LanceVaneSearchTaskVariant::VECTOR_CANDIDATES:
  case LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES:
  case LanceVaneSearchTaskVariant::FTS_CANDIDATES:
    break;
  default:
    throw InternalException("Cannot encode an unknown SearchTaskAssignment "
                            "variant");
  }
  auto has_fragment = assignment.fragment_id.IsValid();
  auto has_index_segment = !assignment.index_segment_uuid.empty();
  if (!IsValidSearchUUID(assignment.search_node_uuid) ||
      assignment.state_sha256.size() != LANCE_VANE_SHA256_SIZE ||
      (assignment.variant == LanceVaneSearchTaskVariant::FINAL_SEARCH &&
       (has_fragment || has_index_segment)) ||
      (assignment.variant == LanceVaneSearchTaskVariant::VECTOR_CANDIDATES &&
       (!has_fragment || has_index_segment)) ||
      (assignment.variant ==
           LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES &&
       (has_fragment == has_index_segment ||
        (has_index_segment && assignment.index_segment_uuid.size() !=
                                  LANCE_VANE_INDEX_SEGMENT_UUID_SIZE))) ||
      (assignment.variant == LanceVaneSearchTaskVariant::FTS_CANDIDATES &&
       (has_fragment || !has_index_segment ||
        assignment.index_segment_uuid.size() !=
            LANCE_VANE_INDEX_SEGMENT_UUID_SIZE))) {
    throw InternalException("Cannot encode a malformed SearchTaskAssignment");
  }
  string payload;
  auto payload_size = LANCE_VANE_SEARCH_TASK_BASE_PAYLOAD_SIZE;
  if (assignment.variant == LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
    payload_size = LANCE_VANE_VECTOR_CANDIDATE_TASK_PAYLOAD_SIZE;
  } else if (assignment.variant ==
             LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES) {
    payload_size = LANCE_VANE_INDEXED_VECTOR_TASK_PAYLOAD_SIZE;
  } else if (assignment.variant == LanceVaneSearchTaskVariant::FTS_CANDIDATES) {
    payload_size = LANCE_VANE_FTS_CANDIDATE_TASK_PAYLOAD_SIZE;
  }
  payload.reserve(payload_size);
  payload.push_back(static_cast<char>(assignment.variant));
  payload.append(assignment.search_node_uuid);
  payload.append(assignment.state_sha256);
  if (assignment.variant == LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
    AppendU64(payload, assignment.fragment_id.GetIndex());
  } else if (assignment.variant ==
             LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES) {
    if (!assignment.index_segment_uuid.empty()) {
      payload.push_back(
          static_cast<char>(LanceVaneIndexedVectorWorkKind::INDEX_SEGMENT));
      payload.append(assignment.index_segment_uuid);
    } else {
      payload.push_back(
          static_cast<char>(LanceVaneIndexedVectorWorkKind::FLAT_FRAGMENT));
      AppendU64(payload, assignment.fragment_id.GetIndex());
      payload.append(sizeof(uint64_t), '\0');
    }
  } else if (assignment.variant == LanceVaneSearchTaskVariant::FTS_CANDIDATES) {
    payload.append(assignment.index_segment_uuid);
  }
  return payload;
}

static LanceVaneSearchTaskAssignment
DecodeSearchTaskAssignment(const string &payload) {
  if (payload.empty()) {
    throw SerializationException(
        "SearchTaskAssignment payload has an invalid size");
  }
  LanceVaneSearchTaskAssignment result;
  auto variant = static_cast<uint8_t>(payload[0]);
  switch (variant) {
  case static_cast<uint8_t>(LanceVaneSearchTaskVariant::FINAL_SEARCH): {
    if (payload.size() != LANCE_VANE_SEARCH_TASK_BASE_PAYLOAD_SIZE) {
      throw SerializationException(
          "SearchTaskAssignment payload has an invalid size");
    }
    result.variant = LanceVaneSearchTaskVariant::FINAL_SEARCH;
    break;
  }
  case static_cast<uint8_t>(LanceVaneSearchTaskVariant::VECTOR_CANDIDATES): {
    if (payload.size() != LANCE_VANE_VECTOR_CANDIDATE_TASK_PAYLOAD_SIZE) {
      throw SerializationException(
          "SearchTaskAssignment payload has an invalid size");
    }
    result.variant = LanceVaneSearchTaskVariant::VECTOR_CANDIDATES;
    result.fragment_id = optional_idx(
        ReadU64(payload, LANCE_VANE_SEARCH_TASK_BASE_PAYLOAD_SIZE));
    break;
  }
  case static_cast<uint8_t>(
      LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES): {
    if (payload.size() != LANCE_VANE_INDEXED_VECTOR_TASK_PAYLOAD_SIZE) {
      throw SerializationException(
          "SearchTaskAssignment payload has an invalid size");
    }
    result.variant = LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES;
    auto work_offset = LANCE_VANE_SEARCH_TASK_BASE_PAYLOAD_SIZE;
    auto work_kind = static_cast<uint8_t>(payload[work_offset]);
    if (work_kind ==
        static_cast<uint8_t>(LanceVaneIndexedVectorWorkKind::INDEX_SEGMENT)) {
      result.index_segment_uuid =
          payload.substr(work_offset + 1, LANCE_VANE_INDEX_SEGMENT_UUID_SIZE);
      if (std::all_of(result.index_segment_uuid.begin(),
                      result.index_segment_uuid.end(),
                      [](char value) { return value == 0; })) {
        throw SerializationException(
            "SearchTaskAssignment payload has an invalid index segment");
      }
    } else if (work_kind ==
               static_cast<uint8_t>(
                   LanceVaneIndexedVectorWorkKind::FLAT_FRAGMENT)) {
      result.fragment_id = optional_idx(ReadU64(payload, work_offset + 1));
      auto padding_offset = work_offset + 1 + sizeof(uint64_t);
      if (!std::all_of(payload.begin() + padding_offset, payload.end(),
                       [](char value) { return value == 0; })) {
        throw SerializationException(
            "SearchTaskAssignment payload has invalid padding");
      }
    } else {
      throw SerializationException(
          "SearchTaskAssignment payload has an invalid work kind");
    }
    break;
  }
  case static_cast<uint8_t>(LanceVaneSearchTaskVariant::FTS_CANDIDATES): {
    if (payload.size() != LANCE_VANE_FTS_CANDIDATE_TASK_PAYLOAD_SIZE) {
      throw SerializationException(
          "SearchTaskAssignment payload has an invalid size");
    }
    result.variant = LanceVaneSearchTaskVariant::FTS_CANDIDATES;
    result.index_segment_uuid =
        payload.substr(LANCE_VANE_SEARCH_TASK_BASE_PAYLOAD_SIZE,
                       LANCE_VANE_INDEX_SEGMENT_UUID_SIZE);
    if (std::all_of(result.index_segment_uuid.begin(),
                    result.index_segment_uuid.end(),
                    [](char value) { return value == 0; })) {
      throw SerializationException(
          "SearchTaskAssignment payload has an invalid index segment");
    }
    break;
  }
  default:
    throw SerializationException(
        "SearchTaskAssignment payload has an invalid variant");
  }
  result.search_node_uuid = payload.substr(1, LANCE_VANE_SEARCH_UUID_SIZE);
  result.state_sha256 =
      payload.substr(1 + LANCE_VANE_SEARCH_UUID_SIZE, LANCE_VANE_SHA256_SIZE);
  if (!IsValidSearchUUID(result.search_node_uuid)) {
    throw SerializationException(
        "SearchTaskAssignment payload has an invalid search identity");
  }
  return result;
}

static LanceVaneSearchTaskAssignment
ExpectedSearchTaskAssignment(const LanceVaneGlobalSearchState &state,
                             optional_idx fragment_id = optional_idx(),
                             const string &index_segment_uuid = string()) {
  return {state.execution_variant, state.search_node_uuid, state.state_sha256,
          fragment_id, index_segment_uuid};
}

static string HexIndexSegmentUUID(const string &uuid) {
  if (uuid.size() != LANCE_VANE_INDEX_SEGMENT_UUID_SIZE) {
    throw InternalException("Cannot format an invalid index segment UUID");
  }
  static constexpr const char *HEX = "0123456789abcdef";
  string result;
  result.reserve(uuid.size() * 2);
  for (auto byte : uuid) {
    auto value = static_cast<uint8_t>(byte);
    result.push_back(HEX[value >> 4]);
    result.push_back(HEX[value & 0x0f]);
  }
  return result;
}

static string
ExpectedSearchTaskId(const LanceVaneGlobalSearchState &state,
                     optional_idx fragment_id = optional_idx(),
                     const string &index_segment_uuid = string()) {
  if (state.execution_variant == LanceVaneSearchTaskVariant::FINAL_SEARCH) {
    if (fragment_id.IsValid() || !index_segment_uuid.empty()) {
      throw InternalException("FINAL_SEARCH cannot identify candidate work");
    }
    return string(LANCE_VANE_FINAL_SEARCH_TASK_ID_PREFIX) +
           state.search_node_uuid;
  }
  if (state.execution_variant ==
          LanceVaneSearchTaskVariant::VECTOR_CANDIDATES &&
      fragment_id.IsValid()) {
    return string(LANCE_VANE_VECTOR_CANDIDATE_TASK_ID_PREFIX) +
           state.search_node_uuid + ":" + to_string(fragment_id.GetIndex());
  }
  if (state.execution_variant ==
      LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES) {
    auto prefix = string(LANCE_VANE_INDEXED_VECTOR_CANDIDATE_TASK_ID_PREFIX) +
                  state.search_node_uuid;
    if (!index_segment_uuid.empty() && !fragment_id.IsValid()) {
      return prefix + ":segment:" + HexIndexSegmentUUID(index_segment_uuid);
    }
    if (index_segment_uuid.empty() && fragment_id.IsValid()) {
      return prefix + ":fragment:" + to_string(fragment_id.GetIndex());
    }
  }
  if (state.execution_variant == LanceVaneSearchTaskVariant::FTS_CANDIDATES &&
      !index_segment_uuid.empty() && !fragment_id.IsValid()) {
    return string(LANCE_VANE_FTS_CANDIDATE_TASK_ID_PREFIX) +
           state.search_node_uuid + ":" +
           HexIndexSegmentUUID(index_segment_uuid);
  }
  throw InternalException("Unsupported distributed search task identity");
}

static bool HasValidFragmentStats(const LanceVaneGlobalSearchState &state) {
  if (state.fragment_ids.size() != state.fragment_row_counts.size() ||
      state.fragment_ids.size() != state.fragment_bytes_on_disk.size()) {
    return false;
  }
  for (idx_t i = 0; i < state.fragment_ids.size(); i++) {
    if (state.fragment_row_counts[i] < -1 ||
        (i > 0 && state.fragment_ids[i - 1] >= state.fragment_ids[i])) {
      return false;
    }
  }
  return true;
}

static bool HasValidIndexedVectorWork(const LanceVaneGlobalSearchState &state) {
  auto &segment_uuids = state.indexed_vector_segment_uuids;
  auto &offsets = state.indexed_vector_segment_fragment_offsets;
  auto &segment_fragments = state.indexed_vector_segment_fragment_ids;
  auto &uncovered = state.indexed_vector_uncovered_fragment_ids;
  if (segment_uuids.empty()) {
    return offsets.empty() && segment_fragments.empty() && uncovered.empty();
  }
  if (offsets.size() != segment_uuids.size() + 1 || offsets.front() != 0 ||
      offsets.back() != segment_fragments.size() ||
      !std::is_sorted(segment_uuids.begin(), segment_uuids.end()) ||
      std::adjacent_find(segment_uuids.begin(), segment_uuids.end()) !=
          segment_uuids.end() ||
      !std::is_sorted(uncovered.begin(), uncovered.end()) ||
      std::adjacent_find(uncovered.begin(), uncovered.end()) !=
          uncovered.end()) {
    return false;
  }

  vector<uint64_t> assigned_fragments;
  assigned_fragments.reserve(segment_fragments.size() + uncovered.size());
  for (idx_t segment_idx = 0; segment_idx < segment_uuids.size();
       segment_idx++) {
    auto &uuid = segment_uuids[segment_idx];
    if (uuid.size() != LANCE_VANE_INDEX_SEGMENT_UUID_SIZE ||
        std::all_of(uuid.begin(), uuid.end(),
                    [](char value) { return value == 0; })) {
      return false;
    }
    auto begin = offsets[segment_idx];
    auto end = offsets[segment_idx + 1];
    if (begin >= end || end > segment_fragments.size()) {
      return false;
    }
    auto first = segment_fragments.begin() + begin;
    auto last = segment_fragments.begin() + end;
    if (!std::is_sorted(first, last) ||
        std::adjacent_find(first, last) != last) {
      return false;
    }
    assigned_fragments.insert(assigned_fragments.end(), first, last);
  }
  assigned_fragments.insert(assigned_fragments.end(), uncovered.begin(),
                            uncovered.end());
  std::sort(assigned_fragments.begin(), assigned_fragments.end());
  if (std::adjacent_find(assigned_fragments.begin(),
                         assigned_fragments.end()) !=
      assigned_fragments.end()) {
    return false;
  }
  return assigned_fragments == state.fragment_ids;
}

static bool HasValidFtsWork(const LanceVaneGlobalSearchState &state) {
  auto &segment_uuids = state.fts_segment_uuids;
  auto &offsets = state.fts_segment_fragment_offsets;
  auto &segment_fragments = state.fts_segment_fragment_ids;
  if (segment_uuids.empty()) {
    return offsets.empty() && segment_fragments.empty();
  }
  if (offsets.size() != segment_uuids.size() + 1 || offsets.front() != 0 ||
      offsets.back() != segment_fragments.size() ||
      !std::is_sorted(segment_uuids.begin(), segment_uuids.end()) ||
      std::adjacent_find(segment_uuids.begin(), segment_uuids.end()) !=
          segment_uuids.end()) {
    return false;
  }

  vector<uint64_t> assigned_fragments;
  assigned_fragments.reserve(segment_fragments.size());
  for (idx_t segment_idx = 0; segment_idx < segment_uuids.size();
       segment_idx++) {
    auto &uuid = segment_uuids[segment_idx];
    if (uuid.size() != LANCE_VANE_INDEX_SEGMENT_UUID_SIZE ||
        std::all_of(uuid.begin(), uuid.end(),
                    [](char value) { return value == 0; })) {
      return false;
    }
    auto begin = offsets[segment_idx];
    auto end = offsets[segment_idx + 1];
    if (begin >= end || end > segment_fragments.size()) {
      return false;
    }
    auto first = segment_fragments.begin() + begin;
    auto last = segment_fragments.begin() + end;
    if (!std::is_sorted(first, last) ||
        std::adjacent_find(first, last) != last) {
      return false;
    }
    assigned_fragments.insert(assigned_fragments.end(), first, last);
  }
  std::sort(assigned_fragments.begin(), assigned_fragments.end());
  if (std::adjacent_find(assigned_fragments.begin(),
                         assigned_fragments.end()) !=
      assigned_fragments.end()) {
    return false;
  }
  return assigned_fragments == state.fragment_ids;
}

static LanceVaneSearchTaskAssignment
ValidateAuthorizedTask(const LanceVaneGlobalSearchState &state,
                       const string &task_id, const string &payload) {
  if (state.execution_variant == LanceVaneSearchTaskVariant::FINAL_SEARCH &&
      task_id != ExpectedSearchTaskId(state)) {
    throw SerializationException(
        "Distributed Lance search received an unauthorized "
        "SearchTaskAssignment identity");
  }
  auto assignment = DecodeSearchTaskAssignment(payload);
  if (assignment.variant != state.execution_variant) {
    throw SerializationException(
        "Distributed Lance search received an unsupported "
        "SearchTaskAssignment variant");
  }
  if (assignment.search_node_uuid != state.search_node_uuid ||
      assignment.state_sha256 != state.state_sha256) {
    throw SerializationException(
        "Distributed Lance search received a SearchTaskAssignment for a "
        "different global state");
  }
  if (state.execution_variant == LanceVaneSearchTaskVariant::FINAL_SEARCH) {
    if (assignment.fragment_id.IsValid() ||
        !assignment.index_segment_uuid.empty()) {
      throw SerializationException(
          "Distributed Lance FINAL_SEARCH authorization is malformed");
    }
    return assignment;
  }
  auto authorized = false;
  if (state.execution_variant ==
      LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
    authorized =
        assignment.fragment_id.IsValid() &&
        assignment.index_segment_uuid.empty() &&
        std::binary_search(state.fragment_ids.begin(), state.fragment_ids.end(),
                           assignment.fragment_id.GetIndex());
  } else if (state.execution_variant ==
             LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES) {
    if (!assignment.index_segment_uuid.empty()) {
      authorized =
          !assignment.fragment_id.IsValid() &&
          std::binary_search(state.indexed_vector_segment_uuids.begin(),
                             state.indexed_vector_segment_uuids.end(),
                             assignment.index_segment_uuid);
    } else if (assignment.fragment_id.IsValid()) {
      authorized = std::binary_search(
          state.indexed_vector_uncovered_fragment_ids.begin(),
          state.indexed_vector_uncovered_fragment_ids.end(),
          assignment.fragment_id.GetIndex());
    }
  } else if (state.execution_variant ==
             LanceVaneSearchTaskVariant::FTS_CANDIDATES) {
    authorized = !assignment.fragment_id.IsValid() &&
                 std::binary_search(state.fts_segment_uuids.begin(),
                                    state.fts_segment_uuids.end(),
                                    assignment.index_segment_uuid);
  }
  if (!authorized ||
      task_id != ExpectedSearchTaskId(state, assignment.fragment_id,
                                      assignment.index_segment_uuid)) {
    throw SerializationException(
        "Distributed Lance search received an unauthorized "
        "SearchTaskAssignment identity");
  }
  return assignment;
}

static void ValidateGlobalSearchState(const LanceVaneGlobalSearchState &state,
                                      bool verify_digest) {
  auto supported_variant =
      state.execution_variant == LanceVaneSearchTaskVariant::FINAL_SEARCH ||
      LanceVaneIsCandidateVariant(state.execution_variant);
  if (state.contract_version != LANCE_VANE_SEARCH_CONTRACT_VERSION ||
      static_cast<uint8_t>(state.source_class) >
          static_cast<uint8_t>(LanceVaneSearchSourceClass::STANDARD_REST) ||
      !IsValidSearchUUID(state.search_node_uuid) ||
      state.output_names.empty() ||
      state.output_names.size() != state.output_types.size() ||
      state.state_sha256.size() != LANCE_VANE_SHA256_SIZE ||
      state.authorized_task_ids.size() !=
          state.authorized_task_payloads.size() ||
      !supported_variant || !HasValidFragmentStats(state) ||
      !HasValidIndexedVectorWork(state) || !HasValidFtsWork(state)) {
    throw SerializationException("Distributed Lance search state is malformed");
  }
  for (auto &name : state.output_names) {
    if (name.empty() || name.find('\0') != string::npos) {
      throw SerializationException(
          "Distributed Lance search output schema is malformed");
    }
  }
  ValidateSearchArguments(state.arguments);
  if (!SearchKindMatchesOverload(state.arguments)) {
    throw SerializationException(
        "Distributed Lance search overload identity mismatch");
  }
  if ((state.valid && state.arguments.namespace_backed &&
       (state.source_class == LanceVaneSearchSourceClass::DIRECT ||
        state.arguments.kind == LanceVaneSearchKind::HYBRID)) ||
      (!state.arguments.namespace_backed &&
       !state.namespace_filter_plan.empty())) {
    throw SerializationException(
        "Distributed Lance search namespace state is contradictory");
  }
  if (!state.valid) {
    if (state.finalized || state.qualification_failure.empty() ||
        state.qualification_failure.find('\0') != string::npos ||
        !state.physical_uri.empty() || state.dataset_version != 0 ||
        !state.dataset_generation_id.empty() ||
        !state.schema_fingerprint.empty() ||
        !state.namespace_filter_plan.empty() || !state.index_plan.empty() ||
        state.search_plan_payloads_validated || !state.column_ids.empty() ||
        !state.projection_ids.empty() || !state.final_filter_ir.empty() ||
        !state.filter_fingerprint.empty() || state.filter_pushed_down ||
        state.worker_bind || state.task_assignment_applied ||
        state.empty_assignment || state.authorization_restricted ||
        !state.authorized_task_ids.empty() || state.frozen_snapshot ||
        state.frozen_snapshot_payload_validated ||
        state.execution_variant != LanceVaneSearchTaskVariant::FINAL_SEARCH ||
        !state.fragment_ids.empty() ||
        !state.indexed_vector_segment_uuids.empty() ||
        !state.fts_segment_uuids.empty() ||
        !state.selected_fragment_ids.empty() ||
        !state.selected_index_segment_uuids.empty()) {
      throw SerializationException(
          "Distributed Lance search qualification failure is malformed");
    }
    if (verify_digest && LanceVaneSha256(CanonicalSearchStateBytes(state)) !=
                             state.state_sha256) {
      throw SerializationException(
          "Distributed Lance search state digest mismatch");
    }
    return;
  }
  if (!state.qualification_failure.empty() || state.physical_uri.empty() ||
      state.physical_uri.find('\0') != string::npos ||
      state.dataset_version == 0 || state.dataset_generation_id.empty() ||
      state.dataset_generation_id.find('\0') != string::npos ||
      state.schema_fingerprint.size() != LANCE_VANE_SHA256_SIZE ||
      state.filter_fingerprint.size() != LANCE_VANE_SHA256_SIZE ||
      !state.search_plan_payloads_validated || !state.frozen_snapshot ||
      !state.frozen_snapshot_payload_validated ||
      state.frozen_snapshot->dataset.schema_fingerprint !=
          state.schema_fingerprint) {
    throw SerializationException("Distributed Lance search state is malformed");
  }
  if (!state.finalized &&
      (!state.column_ids.empty() || !state.projection_ids.empty() ||
       !state.final_filter_ir.empty() || state.filter_pushed_down ||
       state.execution_variant != LanceVaneSearchTaskVariant::FINAL_SEARCH ||
       state.worker_bind || state.task_assignment_applied ||
       state.empty_assignment || state.authorization_restricted ||
       !state.authorized_task_ids.empty() ||
       !state.indexed_vector_segment_uuids.empty() ||
       !state.fts_segment_uuids.empty() ||
       !state.selected_fragment_ids.empty() ||
       !state.selected_index_segment_uuids.empty())) {
    throw SerializationException(
        "Distributed Lance search coordinator state is contradictory");
  }
  if (state.finalized && (!state.pending_filter_ir_parts.empty() ||
                          state.pending_complex_filter_pushdown_failed)) {
    throw SerializationException(
        "Distributed Lance search finalized state retains planning inputs");
  }
  const bool has_no_task_assignment = state.authorized_task_ids.empty() &&
                                      state.authorized_task_payloads.empty();
  if (state.execution_variant ==
      LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
    if (state.arguments.kind != LanceVaneSearchKind::VECTOR ||
        state.arguments.use_index ||
        state.fragment_ids.size() < LANCE_VANE_VECTOR_CANDIDATE_MIN_FRAGMENTS ||
        !state.indexed_vector_segment_uuids.empty() ||
        !state.fts_segment_uuids.empty() ||
        (!state.arguments.prefilter &&
         (!state.final_filter_ir.empty() ||
          !state.namespace_filter_plan.empty()))) {
      throw SerializationException(
          "Distributed Lance vector candidate state is contradictory");
    }
    for (auto row_count : state.fragment_row_counts) {
      if (row_count < 0) {
        throw SerializationException(
            "Distributed Lance vector candidate row counts are incomplete");
      }
    }
  } else if (state.execution_variant ==
             LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES) {
    auto work_count = state.indexed_vector_segment_uuids.size() +
                      state.indexed_vector_uncovered_fragment_ids.size();
    if (state.arguments.kind != LanceVaneSearchKind::VECTOR ||
        !state.arguments.use_index || state.arguments.nprobes == 0 ||
        state.arguments.refine_factor != 0 ||
        state.indexed_vector_segment_uuids.empty() || work_count < 2 ||
        !state.fts_segment_uuids.empty() ||
        (!state.arguments.prefilter &&
         (!state.final_filter_ir.empty() ||
          !state.namespace_filter_plan.empty()))) {
      throw SerializationException(
          "Distributed Lance indexed vector candidate state is "
          "contradictory");
    }
    for (auto row_count : state.fragment_row_counts) {
      if (row_count < 0) {
        throw SerializationException(
            "Distributed Lance indexed vector candidate row counts are "
            "incomplete");
      }
    }
  } else if (state.execution_variant ==
             LanceVaneSearchTaskVariant::FTS_CANDIDATES) {
    if (state.arguments.kind != LanceVaneSearchKind::FTS ||
        state.arguments.namespace_backed ||
        state.source_class != LanceVaneSearchSourceClass::DIRECT ||
        !state.arguments.use_index ||
        !state.indexed_vector_segment_uuids.empty() ||
        state.fts_segment_uuids.size() < 2 || !state.final_filter_ir.empty() ||
        !state.namespace_filter_plan.empty() || state.filter_pushed_down) {
      throw SerializationException(
          "Distributed Lance FTS candidate state is contradictory");
    }
    uint64_t total_rows = 0;
    for (auto row_count : state.fragment_row_counts) {
      if (row_count < 0 ||
          NumericCast<uint64_t>(row_count) >
              NumericLimits<uint64_t>::Maximum() - total_rows) {
        throw SerializationException(
            "Distributed Lance FTS candidate row counts are incomplete");
      }
      total_rows += NumericCast<uint64_t>(row_count);
    }
    if (total_rows < LANCE_VANE_FTS_CANDIDATE_MIN_ROWS) {
      throw SerializationException(
          "Distributed Lance FTS candidate state is below its work threshold");
    }
  } else if (!state.indexed_vector_segment_uuids.empty() ||
             !state.fts_segment_uuids.empty() ||
             !state.selected_fragment_ids.empty() ||
             !state.selected_index_segment_uuids.empty()) {
    throw SerializationException(
        "Distributed Lance FINAL_SEARCH candidate state unexpectedly");
  }
  if (!state.worker_bind &&
      (state.task_assignment_applied || state.empty_assignment ||
       state.authorization_restricted || !has_no_task_assignment ||
       !state.selected_fragment_ids.empty() ||
       !state.selected_index_segment_uuids.empty())) {
    throw SerializationException(
        "Distributed Lance search assignment state is contradictory");
  }
  if (state.worker_bind) {
    if (!state.authorization_restricted ||
        (state.empty_assignment &&
         (!state.task_assignment_applied || !has_no_task_assignment ||
          !state.selected_fragment_ids.empty() ||
          !state.selected_index_segment_uuids.empty())) ||
        (!state.empty_assignment && has_no_task_assignment)) {
      throw SerializationException(
          "Distributed Lance search assignment state is contradictory");
    }
    vector<uint64_t> authorized_fragments;
    vector<string> authorized_segments;
    authorized_fragments.reserve(state.authorized_task_ids.size());
    authorized_segments.reserve(state.authorized_task_ids.size());
    for (idx_t i = 0; i < state.authorized_task_ids.size(); i++) {
      auto assignment =
          ValidateAuthorizedTask(state, state.authorized_task_ids[i],
                                 state.authorized_task_payloads[i]);
      if (assignment.fragment_id.IsValid()) {
        authorized_fragments.push_back(assignment.fragment_id.GetIndex());
      }
      if (!assignment.index_segment_uuid.empty()) {
        authorized_segments.push_back(assignment.index_segment_uuid);
      }
    }
    if (state.execution_variant == LanceVaneSearchTaskVariant::FINAL_SEARCH) {
      if (!state.empty_assignment && state.authorized_task_ids.size() != 1) {
        throw SerializationException(
            "Distributed Lance FINAL_SEARCH authorization is contradictory");
      }
    } else if (state.execution_variant ==
               LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
      if (!std::is_sorted(authorized_fragments.begin(),
                          authorized_fragments.end()) ||
          std::adjacent_find(authorized_fragments.begin(),
                             authorized_fragments.end()) !=
              authorized_fragments.end()) {
        throw SerializationException(
            "Distributed Lance vector candidate authorization is not unique");
      }
      if (!state.task_assignment_applied) {
        if (authorized_fragments != state.fragment_ids ||
            !state.selected_fragment_ids.empty()) {
          throw SerializationException(
              "Distributed Lance vector candidate preauthorization is "
              "contradictory");
        }
      } else if (!state.empty_assignment &&
                 authorized_fragments != state.selected_fragment_ids) {
        throw SerializationException(
            "Distributed Lance vector candidate assignment is contradictory");
      }
      if (!authorized_segments.empty() ||
          !state.selected_index_segment_uuids.empty()) {
        throw SerializationException(
            "Distributed Lance exact vector candidate segments are "
            "contradictory");
      }
    } else if (state.execution_variant ==
               LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES) {
      if (!std::is_sorted(authorized_fragments.begin(),
                          authorized_fragments.end()) ||
          std::adjacent_find(authorized_fragments.begin(),
                             authorized_fragments.end()) !=
              authorized_fragments.end() ||
          !std::is_sorted(authorized_segments.begin(),
                          authorized_segments.end()) ||
          std::adjacent_find(authorized_segments.begin(),
                             authorized_segments.end()) !=
              authorized_segments.end()) {
        throw SerializationException(
            "Distributed Lance indexed vector candidate authorization is not "
            "unique");
      }
      if (!state.task_assignment_applied) {
        if (authorized_fragments !=
                state.indexed_vector_uncovered_fragment_ids ||
            authorized_segments != state.indexed_vector_segment_uuids ||
            !state.selected_fragment_ids.empty() ||
            !state.selected_index_segment_uuids.empty()) {
          throw SerializationException(
              "Distributed Lance indexed vector candidate preauthorization "
              "is contradictory");
        }
      } else if (!state.empty_assignment &&
                 (authorized_fragments != state.selected_fragment_ids ||
                  authorized_segments != state.selected_index_segment_uuids)) {
        throw SerializationException(
            "Distributed Lance indexed vector candidate assignment is "
            "contradictory");
      }
    } else {
      if (!authorized_fragments.empty() ||
          !state.selected_fragment_ids.empty() ||
          !std::is_sorted(authorized_segments.begin(),
                          authorized_segments.end()) ||
          std::adjacent_find(authorized_segments.begin(),
                             authorized_segments.end()) !=
              authorized_segments.end()) {
        throw SerializationException(
            "Distributed Lance FTS candidate authorization is not unique");
      }
      if (!state.task_assignment_applied) {
        if (authorized_segments != state.fts_segment_uuids ||
            !state.selected_index_segment_uuids.empty()) {
          throw SerializationException(
              "Distributed Lance FTS candidate preauthorization is "
              "contradictory");
        }
      } else if (!state.empty_assignment &&
                 authorized_segments != state.selected_index_segment_uuids) {
        throw SerializationException(
            "Distributed Lance FTS candidate assignment is contradictory");
      }
    }
  }
  if (verify_digest &&
      LanceVaneSha256(CanonicalSearchStateBytes(state)) != state.state_sha256) {
    throw SerializationException(
        "Distributed Lance search state digest mismatch");
  }
}

void LanceVaneCapturePhysicalCandidate(
    ClientContext &context, const string &physical_uri,
    const shared_ptr<LanceDatasetCacheEntry> &dataset_entry,
    LanceVaneSearchSourceClass source_class, bool private_uri_diagnostics,
    bool requires_coordinator_storage_secret,
    LanceVanePhysicalCandidate &out_candidate) {
  out_candidate = {};
  out_candidate.attempted = true;
  out_candidate.source_class = source_class;
  out_candidate.private_uri_diagnostics = private_uri_diagnostics;
  if (requires_coordinator_storage_secret) {
    out_candidate.safe_failure =
        "Distributed Lance search cannot use a coordinator-only storage "
        "secret";
    return;
  }
  auto replay_path = LanceVaneReplayPath(context, physical_uri);
  if (replay_path.empty()) {
    out_candidate.safe_failure =
        "Distributed Lance search requires a replayable physical dataset URI";
    return;
  }
  if (!dataset_entry || !dataset_entry->Handle()) {
    out_candidate.safe_failure =
        "Distributed Lance search could not freeze a physical dataset";
    return;
  }
  auto dataset = dataset_entry->Handle();
  auto version = lance_dataset_version(dataset);
  auto *generation_ptr = lance_dataset_generation_id(dataset);
  if (version == 0 || !generation_ptr) {
    if (generation_ptr) {
      lance_free_string(generation_ptr);
    }
    out_candidate.safe_failure =
        "Distributed Lance search could not freeze an exact snapshot";
    return;
  }
  string generation = generation_ptr;
  lance_free_string(generation_ptr);
  if (generation.empty() || generation.find('\0') != string::npos) {
    out_candidate.safe_failure =
        "Distributed Lance search received an invalid snapshot identity";
    return;
  }
  string schema_fingerprint(LANCE_VANE_SHA256_SIZE, '\0');
  if (lance_vane_dataset_schema_fingerprint(
          dataset, reinterpret_cast<uint8_t *>(&schema_fingerprint[0])) != 0) {
    LanceConsumeLastError();
    out_candidate.safe_failure =
        "Distributed Lance search could not fingerprint the physical schema";
    return;
  }

  out_candidate.qualified = true;
  out_candidate.physical_uri = std::move(replay_path);
  out_candidate.dataset_version = version;
  out_candidate.dataset_generation_id = std::move(generation);
  out_candidate.schema_fingerprint = std::move(schema_fingerprint);
  out_candidate.search_node_uuid = UUID::ToString(UUID::GenerateRandomUUID());
  out_candidate.dataset_entry = dataset_entry;
  out_candidate.dataset = dataset;
  out_candidate.context = &context;
}

static string BuildIndexPlan(const LanceVanePhysicalCandidate &candidate,
                             const LanceVaneSearchArguments &arguments,
                             const LanceVaneFrozenSearchSnapshot &snapshot) {
  if (!candidate.context) {
    throw InternalException(
        "Distributed Lance search lost its coordinator context");
  }
  unique_ptr<void, LanceVanePlanningDatasetDeleter> planning_dataset(
      LanceOpenDatasetVersionFromManifestAndIndexSectionForDistributedSearch(
          *candidate.context, candidate.physical_uri, candidate.dataset_version,
          snapshot.dataset.serialized_manifest,
          snapshot.serialized_index_section, candidate.dataset_generation_id));
  if (!planning_dataset) {
    LanceConsumeLastError();
    throw IOException(
        "Failed to open the isolated distributed Lance planning snapshot" +
        LanceVaneFormatErrorSuffix(candidate.physical_uri,
                                   candidate.private_uri_diagnostics));
  }
  uint8_t *bytes = nullptr;
  size_t len = 0;
  auto *vector_column = arguments.vector_column.empty()
                            ? nullptr
                            : arguments.vector_column.c_str();
  auto *text_column =
      arguments.text_column.empty() ? nullptr : arguments.text_column.c_str();
  auto rc = lance_vane_build_search_index_plan(
      planning_dataset.get(), candidate.dataset_generation_id.c_str(),
      static_cast<uint8_t>(arguments.kind), vector_column, text_column,
      arguments.use_index ? 1 : 0, &bytes, &len);
  if (rc != 0 || !bytes || len == 0) {
    if (bytes) {
      lance_vane_free_bytes(bytes, len);
    }
    throw IOException(
        "Failed to freeze the distributed Lance search index plan" +
        LanceVaneFormatErrorSuffix(candidate.physical_uri,
                                   candidate.private_uri_diagnostics));
  }
  string result(reinterpret_cast<const char *>(bytes), len);
  lance_vane_free_bytes(bytes, len);
  return result;
}

static shared_ptr<const LanceVaneFrozenSearchSnapshot>
FreezeSearchSnapshot(const LanceVanePhysicalCandidate &candidate) {
  auto result = make_shared_ptr<LanceVaneFrozenSearchSnapshot>();
  result->dataset =
      LanceVaneFreezeSnapshot(candidate.dataset, candidate.physical_uri,
                              candidate.private_uri_diagnostics);
  if (result->dataset.schema_fingerprint != candidate.schema_fingerprint) {
    throw IOException(
        "Distributed Lance search schema changed while freezing its snapshot");
  }

  uint8_t *index_section = nullptr;
  size_t index_section_len = 0;
  auto rc = lance_vane_serialize_dataset_index_section(
      candidate.dataset, &index_section, &index_section_len);
  unique_ptr<uint8_t, LanceVaneSearchBytesDeleter> index_section_owner(
      index_section, LanceVaneSearchBytesDeleter{index_section_len});
  if (rc != 0 || (index_section_len > 0 && !index_section) ||
      index_section_len > LANCE_VANE_MAX_SERIALIZED_INDEX_SECTION_BYTES) {
    throw IOException(
        "Failed to freeze the distributed Lance search index metadata" +
        LanceVaneFormatErrorSuffix(candidate.physical_uri,
                                   candidate.private_uri_diagnostics));
  }
  if (index_section_len > 0) {
    result->serialized_index_section.assign(
        reinterpret_cast<const char *>(index_section_owner.get()),
        index_section_len);
  }
  result->index_section_sha256 =
      LanceVaneSha256(result->serialized_index_section);
  return result;
}

struct LanceVaneFragmentStatsDeleter {
  size_t len;

  void operator()(LanceFragmentStats *value) const {
    if (value) {
      lance_free_fragment_stats_list(value, len);
    }
  }
};

struct LanceVaneIndexedVectorWorkDeleter {
  size_t len;

  void operator()(LanceVaneIndexedVectorWorkFragment *value) const {
    if (value) {
      lance_vane_free_indexed_vector_work(value, len);
    }
  }
};

struct LanceVaneFtsWorkDeleter {
  size_t len;

  void operator()(LanceVaneFtsWorkFragment *value) const {
    if (value) {
      lance_vane_free_fts_work(value, len);
    }
  }
};

struct LanceVaneIndexedVectorWork {
  vector<string> segment_uuids;
  vector<uint64_t> segment_fragment_offsets;
  vector<uint64_t> segment_fragment_ids;
  vector<uint64_t> uncovered_fragment_ids;
};

struct LanceVaneFtsWork {
  vector<string> segment_uuids;
  vector<uint64_t> segment_fragment_offsets;
  vector<uint64_t> segment_fragment_ids;
};

static bool TryPlanIndexedVectorWork(const LanceVaneGlobalSearchState &state,
                                     LanceVaneIndexedVectorWork &result) {
  LanceVaneIndexedVectorWorkFragment *work = nullptr;
  size_t work_len = 0;
  auto rc = lance_vane_plan_indexed_vector_work(
      reinterpret_cast<const uint8_t *>(state.index_plan.data()),
      state.index_plan.size(), &work, &work_len);
  unique_ptr<LanceVaneIndexedVectorWorkFragment,
             LanceVaneIndexedVectorWorkDeleter>
      work_owner(work, LanceVaneIndexedVectorWorkDeleter{work_len});
  if (rc != 0 || (work_len > 0 && !work)) {
    throw IOException("Failed to plan distributed indexed vector work" +
                      LanceFormatErrorSuffix());
  }
  if (work_len == 0) {
    return false;
  }

  LanceVaneIndexedVectorWork planned;
  planned.segment_fragment_offsets.push_back(0);
  auto reached_uncovered = false;
  for (idx_t work_idx = 0; work_idx < work_len; work_idx++) {
    auto &item = work[work_idx];
    string segment_uuid(reinterpret_cast<const char *>(item.segment_uuid),
                        LANCE_VANE_INDEX_SEGMENT_UUID_SIZE);
    auto is_uncovered = std::all_of(segment_uuid.begin(), segment_uuid.end(),
                                    [](char value) { return value == 0; });
    if (is_uncovered) {
      reached_uncovered = true;
      planned.uncovered_fragment_ids.push_back(item.fragment_id);
      continue;
    }
    if (reached_uncovered) {
      throw SerializationException(
          "Distributed indexed vector work is not canonical");
    }
    if (planned.segment_uuids.empty() ||
        planned.segment_uuids.back() != segment_uuid) {
      if (!planned.segment_uuids.empty()) {
        planned.segment_fragment_offsets.push_back(
            planned.segment_fragment_ids.size());
      }
      planned.segment_uuids.push_back(std::move(segment_uuid));
    }
    planned.segment_fragment_ids.push_back(item.fragment_id);
  }
  if (planned.segment_uuids.empty()) {
    return false;
  }
  planned.segment_fragment_offsets.push_back(
      planned.segment_fragment_ids.size());
  result = std::move(planned);
  return true;
}

static bool TryPlanFtsWork(const LanceVaneGlobalSearchState &state,
                           LanceVaneFtsWork &result) {
  LanceVaneFtsWorkFragment *work = nullptr;
  size_t work_len = 0;
  auto rc = lance_vane_plan_fts_work(
      reinterpret_cast<const uint8_t *>(state.index_plan.data()),
      state.index_plan.size(), &work, &work_len);
  unique_ptr<LanceVaneFtsWorkFragment, LanceVaneFtsWorkDeleter> work_owner(
      work, LanceVaneFtsWorkDeleter{work_len});
  if (rc != 0 || (work_len > 0 && !work)) {
    throw IOException("Failed to plan distributed FTS work" +
                      LanceFormatErrorSuffix());
  }
  if (work_len == 0) {
    return false;
  }

  LanceVaneFtsWork planned;
  planned.segment_fragment_offsets.push_back(0);
  for (idx_t work_idx = 0; work_idx < work_len; work_idx++) {
    auto &item = work[work_idx];
    string segment_uuid(reinterpret_cast<const char *>(item.segment_uuid),
                        LANCE_VANE_INDEX_SEGMENT_UUID_SIZE);
    if (std::all_of(segment_uuid.begin(), segment_uuid.end(),
                    [](char value) { return value == 0; })) {
      throw SerializationException("Distributed FTS work has a zero segment");
    }
    if (planned.segment_uuids.empty() ||
        planned.segment_uuids.back() != segment_uuid) {
      if (!planned.segment_uuids.empty()) {
        planned.segment_fragment_offsets.push_back(
            planned.segment_fragment_ids.size());
      }
      planned.segment_uuids.push_back(std::move(segment_uuid));
    }
    planned.segment_fragment_ids.push_back(item.fragment_id);
  }
  planned.segment_fragment_offsets.push_back(
      planned.segment_fragment_ids.size());
  result = std::move(planned);
  return true;
}

static void
FreezeCandidateFragmentStats(const LanceVanePhysicalCandidate &candidate,
                             LanceVaneGlobalSearchState &state) {
  auto vector_candidate =
      state.arguments.kind == LanceVaneSearchKind::VECTOR &&
      (!state.arguments.use_index ||
       (state.arguments.nprobes != 0 && state.arguments.refine_factor == 0));
  LanceVaneFtsWork fts_work;
  auto fts_candidate =
      state.arguments.kind == LanceVaneSearchKind::FTS &&
      !state.arguments.namespace_backed &&
      state.source_class == LanceVaneSearchSourceClass::DIRECT &&
      TryPlanFtsWork(state, fts_work) && fts_work.segment_uuids.size() >= 2;
  if (!vector_candidate && !fts_candidate) {
    return;
  }
  size_t stats_len = 0;
  auto *stats = lance_dataset_list_distributed_fragment_stats(candidate.dataset,
                                                              &stats_len);
  if (!stats) {
    throw IOException(
        "Failed to freeze distributed Lance search fragment statistics" +
        LanceVaneFormatErrorSuffix(candidate.physical_uri,
                                   candidate.private_uri_diagnostics));
  }
  unique_ptr<LanceFragmentStats, LanceVaneFragmentStatsDeleter> stats_owner(
      stats, LanceVaneFragmentStatsDeleter{stats_len});
  vector<LanceFragmentStats> ordered(stats, stats + stats_len);
  std::sort(
      ordered.begin(), ordered.end(),
      [](const LanceFragmentStats &left, const LanceFragmentStats &right) {
        return left.fragment_id < right.fragment_id;
      });
  state.fragment_ids.reserve(ordered.size());
  state.fragment_row_counts.reserve(ordered.size());
  state.fragment_bytes_on_disk.reserve(ordered.size());
  for (auto &fragment : ordered) {
    if (!state.fragment_ids.empty() &&
        state.fragment_ids.back() == fragment.fragment_id) {
      throw IOException(
          "Distributed Lance search fragment identities are not unique");
    }
    state.fragment_ids.push_back(fragment.fragment_id);
    state.fragment_row_counts.push_back(fragment.num_rows);
    state.fragment_bytes_on_disk.push_back(fragment.bytes_on_disk);
  }
}

static string
BuildNamespaceFilterPlan(const LanceVanePhysicalCandidate &candidate,
                         const string &namespace_filter) {
  if (namespace_filter.empty()) {
    return string();
  }
  uint8_t *bytes = nullptr;
  size_t len = 0;
  auto rc = lance_vane_plan_namespace_filter(
      candidate.dataset, namespace_filter.c_str(), &bytes, &len);
  if (rc != 0 || !bytes || len == 0) {
    if (bytes) {
      lance_vane_free_bytes(bytes, len);
    }
    throw InvalidInputException(
        "Distributed Lance search cannot reproduce the namespace filter "
        "against the frozen physical schema");
  }
  string result(reinterpret_cast<const char *>(bytes), len);
  lance_vane_free_bytes(bytes, len);
  return result;
}

LanceVaneGlobalSearchState
LanceVanePrepareGlobalSearchState(const LanceVanePhysicalCandidate &candidate,
                                  const LanceVaneSearchArguments &arguments,
                                  const vector<string> &output_names,
                                  const vector<LogicalType> &output_types) {
  ValidateSearchArguments(arguments);
  if (output_names.empty() || output_names.size() != output_types.size()) {
    throw InternalException(
        "Distributed Lance search has an invalid bound output schema");
  }

  LanceVaneGlobalSearchState state;
  state.arguments = arguments;
  state.arguments.namespace_filter.clear();
  state.output_names = output_names;
  state.output_types = output_types;
  state.search_node_uuid = candidate.search_node_uuid.empty()
                               ? UUID::ToString(UUID::GenerateRandomUUID())
                               : candidate.search_node_uuid;
  state.source_class = candidate.source_class;
  state.private_uri_diagnostics = candidate.private_uri_diagnostics;
  if (!candidate.attempted || !candidate.qualified || !candidate.dataset) {
    state.qualification_failure =
        candidate.safe_failure.empty()
            ? "Distributed Lance search has no qualified physical source"
            : candidate.safe_failure;
    state.state_sha256 = LanceVaneSha256(CanonicalSearchStateBytes(state));
    ValidateGlobalSearchState(state, true);
    return state;
  }

  try {
    state.valid = true;
    state.physical_uri = candidate.physical_uri;
    state.dataset_version = candidate.dataset_version;
    state.dataset_generation_id = candidate.dataset_generation_id;
    state.schema_fingerprint = candidate.schema_fingerprint;
    state.frozen_snapshot = FreezeSearchSnapshot(candidate);
    ValidateAndMarkFrozenSearchSnapshot(state);
    state.namespace_filter_plan =
        BuildNamespaceFilterPlan(candidate, arguments.namespace_filter);
    state.filter_fingerprint =
        BuildFilterFingerprint(output_names, vector<ColumnIndex>(), nullptr);
    state.index_plan =
        BuildIndexPlan(candidate, arguments, *state.frozen_snapshot);
    ValidateAndMarkSearchPlanPayloads(state);
    FreezeCandidateFragmentStats(candidate, state);
  } catch (Exception &) {
    state = {};
    state.arguments = arguments;
    state.arguments.namespace_filter.clear();
    state.output_names = output_names;
    state.output_types = output_types;
    state.search_node_uuid = candidate.search_node_uuid.empty()
                                 ? UUID::ToString(UUID::GenerateRandomUUID())
                                 : candidate.search_node_uuid;
    state.source_class = candidate.source_class;
    state.private_uri_diagnostics = candidate.private_uri_diagnostics;
    state.qualification_failure =
        "Distributed Lance search could not freeze its portable plan";
  }
  state.state_sha256 = LanceVaneSha256(CanonicalSearchStateBytes(state));
  ValidateGlobalSearchState(state, true);
  return state;
}

void LanceVaneAccumulatePendingGlobalSearchFilters(
    LanceVaneGlobalSearchState &state,
    const vector<string> &pushed_filter_ir_parts,
    bool complex_filter_pushdown_failed) {
  if (state.finalized || state.worker_bind) {
    if (!pushed_filter_ir_parts.empty() || complex_filter_pushdown_failed) {
      throw SerializationException(
          "Distributed Lance search worker state cannot change filters");
    }
    return;
  }
  state.pending_filter_ir_parts.insert(state.pending_filter_ir_parts.end(),
                                       pushed_filter_ir_parts.begin(),
                                       pushed_filter_ir_parts.end());
  state.pending_complex_filter_pushdown_failed =
      state.pending_complex_filter_pushdown_failed ||
      complex_filter_pushdown_failed;
  state.state_sha256 = LanceVaneSha256(CanonicalSearchStateBytes(state));
  ValidateGlobalSearchState(state, true);
}

LanceVaneGlobalSearchState LanceVaneFinalizeGlobalSearchState(
    const TableFunctionDistributedScanInput &input,
    const LanceVaneGlobalSearchState &prepared_state,
    const vector<string> &pushed_filter_ir_parts,
    bool complex_filter_pushdown_failed) {
  ValidateGlobalSearchState(prepared_state, true);
  if (!prepared_state.valid) {
    throw NotImplementedException(prepared_state.qualification_failure);
  }
  if (prepared_state.finalized) {
    LanceVaneValidateDistributedInput(input, prepared_state);
    return prepared_state;
  }

  auto state = prepared_state;
  LanceFilterIRBuildResult table_filters;
  auto namespace_search = state.arguments.namespace_backed;
  if (!namespace_search) {
    auto *filters = const_cast<TableFilterSet *>(input.table_filters.get());
    TableFunctionInitInput filter_input(input.bind_data, input.column_ids,
                                        input.projection_ids, filters);
    table_filters = BuildLanceTableFilterIRParts(
        state.output_names, state.output_types, filter_input, true);
  }

  vector<string> complex_filter_parts = state.pending_filter_ir_parts;
  complex_filter_parts.insert(complex_filter_parts.end(),
                              pushed_filter_ir_parts.begin(),
                              pushed_filter_ir_parts.end());
  auto all_complex_filters_pushed =
      namespace_search || (!complex_filter_pushdown_failed &&
                           !state.pending_complex_filter_pushdown_failed);
  auto shared_search = state.arguments.kind != LanceVaneSearchKind::VECTOR;
  if (!namespace_search && shared_search && all_complex_filters_pushed) {
    // Native FTS/hybrid consumes TableFilterSet but not the separately encoded
    // complex-filter parts. Admit only predicates already represented by the
    // same TableFilter IR and keep every other predicate as a DuckDB
    // postfilter.
    for (auto &part : complex_filter_parts) {
      if (std::find(table_filters.parts.begin(), table_filters.parts.end(),
                    part) == table_filters.parts.end()) {
        all_complex_filters_pushed = false;
        break;
      }
    }
  }
  if (state.arguments.prefilter &&
      (!table_filters.all_prefilterable_filters_pushed ||
       !all_complex_filters_pushed)) {
    throw InvalidInputException(
        "Distributed Lance search requires complete filter pushdown when "
        "prefilter=true");
  }

  auto filter_parts = std::move(table_filters.parts);
  if (!namespace_search && !shared_search) {
    filter_parts.insert(filter_parts.end(), complex_filter_parts.begin(),
                        complex_filter_parts.end());
  }
  string filter_ir;
  if (!filter_parts.empty() &&
      !TryEncodeLanceFilterIRMessage(filter_parts, filter_ir)) {
    if (state.arguments.prefilter) {
      throw IOException("Failed to encode the distributed Lance search filter");
    }
    filter_ir.clear();
  }

  state.finalized = true;
  state.column_ids = input.column_ids;
  state.projection_ids = input.projection_ids;
  state.final_filter_ir = std::move(filter_ir);
  state.filter_fingerprint = BuildFilterFingerprint(
      state.output_names, input.column_ids, input.table_filters);
  state.filter_pushed_down =
      !namespace_search && table_filters.all_filters_pushed &&
      all_complex_filters_pushed && !state.final_filter_ir.empty();
  state.pending_filter_ir_parts.clear();
  state.pending_complex_filter_pushdown_failed = false;
  state.state_sha256 = LanceVaneSha256(CanonicalSearchStateBytes(state));
  ValidateGlobalSearchState(state, true);
  return state;
}

bool LanceVaneTryEnableVectorCandidates(LanceVaneGlobalSearchState &state,
                                        bool has_postfilter) {
  ValidateGlobalSearchState(state, true);
  if (!state.valid || !state.finalized || state.worker_bind ||
      state.execution_variant != LanceVaneSearchTaskVariant::FINAL_SEARCH ||
      state.arguments.kind != LanceVaneSearchKind::VECTOR ||
      state.output_names.empty() || state.output_types.empty() ||
      state.output_names.back() != "_distance" ||
      state.output_types.back() != LogicalType::FLOAT || has_postfilter ||
      (!state.arguments.prefilter && (!state.final_filter_ir.empty() ||
                                      !state.namespace_filter_plan.empty()))) {
    return false;
  }

  if (!state.arguments.use_index &&
      state.fragment_ids.size() < LANCE_VANE_VECTOR_CANDIDATE_MIN_FRAGMENTS) {
    return false;
  }

  uint64_t total_rows = 0;
  for (auto row_count : state.fragment_row_counts) {
    if (row_count < 0) {
      return false;
    }
    auto rows = static_cast<uint64_t>(row_count);
    if (rows > NumericLimits<uint64_t>::Maximum() - total_rows) {
      return false;
    }
    total_rows += rows;
  }
  auto query_dimension =
      NumericCast<uint64_t>(state.arguments.vector_query.size());
  if (query_dimension == 0 ||
      total_rows > NumericLimits<uint64_t>::Maximum() / query_dimension ||
      total_rows * query_dimension <
          LANCE_VANE_VECTOR_CANDIDATE_MIN_DISTANCE_VALUES) {
    return false;
  }

  if (!state.arguments.use_index) {
    state.execution_variant = LanceVaneSearchTaskVariant::VECTOR_CANDIDATES;
  } else {
    if (state.arguments.nprobes == 0 || state.arguments.refine_factor != 0) {
      return false;
    }
    LanceVaneIndexedVectorWork work;
    if (!TryPlanIndexedVectorWork(state, work)) {
      return false;
    }
    auto work_count =
        work.segment_uuids.size() + work.uncovered_fragment_ids.size();
    if (work_count < 2) {
      return false;
    }
    state.indexed_vector_segment_uuids = std::move(work.segment_uuids);
    state.indexed_vector_segment_fragment_offsets =
        std::move(work.segment_fragment_offsets);
    state.indexed_vector_segment_fragment_ids =
        std::move(work.segment_fragment_ids);
    state.indexed_vector_uncovered_fragment_ids =
        std::move(work.uncovered_fragment_ids);
    state.execution_variant =
        LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES;
  }
  state.state_sha256 = LanceVaneSha256(CanonicalSearchStateBytes(state));
  ValidateGlobalSearchState(state, true);
  return true;
}

bool LanceVaneTryEnableFtsCandidates(LanceVaneGlobalSearchState &state,
                                     bool has_filter) {
  ValidateGlobalSearchState(state, true);
  if (!state.valid || !state.finalized || state.worker_bind ||
      state.execution_variant != LanceVaneSearchTaskVariant::FINAL_SEARCH ||
      state.arguments.kind != LanceVaneSearchKind::FTS ||
      state.arguments.namespace_backed ||
      state.source_class != LanceVaneSearchSourceClass::DIRECT ||
      !state.arguments.use_index || has_filter ||
      !state.final_filter_ir.empty() || !state.namespace_filter_plan.empty() ||
      state.filter_pushed_down || state.output_names.empty() ||
      state.output_types.empty() || state.output_names.back() != "_score" ||
      state.output_types.back() != LogicalType::FLOAT) {
    return false;
  }

  uint64_t total_rows = 0;
  for (auto row_count : state.fragment_row_counts) {
    if (row_count < 0 || NumericCast<uint64_t>(row_count) >
                             NumericLimits<uint64_t>::Maximum() - total_rows) {
      return false;
    }
    total_rows += NumericCast<uint64_t>(row_count);
  }
  if (total_rows < LANCE_VANE_FTS_CANDIDATE_MIN_ROWS) {
    return false;
  }

  LanceVaneFtsWork work;
  if (!TryPlanFtsWork(state, work) || work.segment_uuids.size() < 2) {
    return false;
  }
  state.fts_segment_uuids = std::move(work.segment_uuids);
  state.fts_segment_fragment_offsets = std::move(work.segment_fragment_offsets);
  state.fts_segment_fragment_ids = std::move(work.segment_fragment_ids);
  state.execution_variant = LanceVaneSearchTaskVariant::FTS_CANDIDATES;
  state.state_sha256 = LanceVaneSha256(CanonicalSearchStateBytes(state));
  ValidateGlobalSearchState(state, true);
  return true;
}

static DistributedScanSplit
BuildSearchTaskAssignment(const LanceVaneGlobalSearchState &state,
                          optional_idx fragment_id = optional_idx(),
                          const string &index_segment_uuid = string()) {
  DistributedScanSplit split;
  split.split_id = ExpectedSearchTaskId(state, fragment_id, index_segment_uuid);
  split.payload = EncodeSearchTaskAssignment(
      ExpectedSearchTaskAssignment(state, fragment_id, index_segment_uuid));
  auto cardinality = state.arguments.k;
  if (fragment_id.IsValid()) {
    if (state.execution_variant ==
            LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES &&
        !std::binary_search(state.indexed_vector_uncovered_fragment_ids.begin(),
                            state.indexed_vector_uncovered_fragment_ids.end(),
                            fragment_id.GetIndex())) {
      throw InternalException(
          "Cannot build an indexed vector candidate task for a covered "
          "fragment");
    }
    auto entry =
        std::lower_bound(state.fragment_ids.begin(), state.fragment_ids.end(),
                         fragment_id.GetIndex());
    if (entry == state.fragment_ids.end() || *entry != fragment_id.GetIndex()) {
      throw InternalException(
          "Cannot build a vector candidate task for an unknown fragment");
    }
    auto index = NumericCast<idx_t>(entry - state.fragment_ids.begin());
    auto row_count = state.fragment_row_counts[index];
    if (row_count >= 0) {
      cardinality =
          MinValue<uint64_t>(cardinality, NumericCast<uint64_t>(row_count));
    }
    if (state.fragment_bytes_on_disk[index] > 0) {
      split.estimated_bytes = optional_idx(state.fragment_bytes_on_disk[index]);
    }
  } else if (!index_segment_uuid.empty()) {
    auto &segment_uuids =
        state.execution_variant == LanceVaneSearchTaskVariant::FTS_CANDIDATES
            ? state.fts_segment_uuids
            : state.indexed_vector_segment_uuids;
    auto &segment_offsets =
        state.execution_variant == LanceVaneSearchTaskVariant::FTS_CANDIDATES
            ? state.fts_segment_fragment_offsets
            : state.indexed_vector_segment_fragment_offsets;
    auto &segment_fragments =
        state.execution_variant == LanceVaneSearchTaskVariant::FTS_CANDIDATES
            ? state.fts_segment_fragment_ids
            : state.indexed_vector_segment_fragment_ids;
    auto segment = std::lower_bound(segment_uuids.begin(), segment_uuids.end(),
                                    index_segment_uuid);
    if (segment == segment_uuids.end() || *segment != index_segment_uuid) {
      throw InternalException(
          "Cannot build a search candidate task for an unknown segment");
    }
    auto segment_idx = NumericCast<idx_t>(segment - segment_uuids.begin());
    auto begin = NumericCast<idx_t>(segment_offsets[segment_idx]);
    auto end = NumericCast<idx_t>(segment_offsets[segment_idx + 1]);
    uint64_t rows = 0;
    uint64_t bytes = 0;
    auto bytes_known = true;
    for (idx_t fragment_idx = begin; fragment_idx < end; fragment_idx++) {
      auto fragment_id = segment_fragments[fragment_idx];
      auto fragment = std::lower_bound(state.fragment_ids.begin(),
                                       state.fragment_ids.end(), fragment_id);
      if (fragment == state.fragment_ids.end() || *fragment != fragment_id) {
        throw InternalException(
            "Search candidate segment references an unknown fragment");
      }
      auto stats_idx =
          NumericCast<idx_t>(fragment - state.fragment_ids.begin());
      auto row_count = state.fragment_row_counts[stats_idx];
      if (row_count < 0 || NumericCast<uint64_t>(row_count) >
                               NumericLimits<uint64_t>::Maximum() - rows) {
        throw InternalException(
            "Search candidate segment has invalid row statistics");
      }
      rows += NumericCast<uint64_t>(row_count);
      auto fragment_bytes = state.fragment_bytes_on_disk[stats_idx];
      if (fragment_bytes == 0 ||
          fragment_bytes > NumericLimits<uint64_t>::Maximum() - bytes) {
        bytes_known = false;
      } else if (bytes_known) {
        bytes += fragment_bytes;
      }
    }
    cardinality = MinValue<uint64_t>(cardinality, rows);
    if (bytes_known && bytes > 0) {
      split.estimated_bytes = optional_idx(bytes);
    }
  }
  split.estimated_cardinality = optional_idx(NumericCast<idx_t>(cardinality));
  split.Validate();
  return split;
}

vector<DistributedScanSplit>
LanceVaneCreateSearchTaskAssignments(const LanceVaneGlobalSearchState &state) {
  ValidateGlobalSearchState(state, true);
  if (!state.finalized) {
    throw InvalidInputException(
        "Distributed Lance search was not finalized before task planning");
  }
  if (state.empty_assignment) {
    return {};
  }
  if (state.authorization_restricted) {
    vector<DistributedScanSplit> result;
    result.reserve(state.authorized_task_ids.size());
    for (idx_t i = 0; i < state.authorized_task_ids.size(); i++) {
      auto assignment =
          ValidateAuthorizedTask(state, state.authorized_task_ids[i],
                                 state.authorized_task_payloads[i]);
      auto split = BuildSearchTaskAssignment(state, assignment.fragment_id,
                                             assignment.index_segment_uuid);
      if (split.split_id != state.authorized_task_ids[i] ||
          split.payload != state.authorized_task_payloads[i]) {
        throw InvalidInputException(
            "Distributed Lance search clone changed authorization");
      }
      result.push_back(std::move(split));
    }
    return result;
  }
  if (state.execution_variant == LanceVaneSearchTaskVariant::FINAL_SEARCH) {
    return {BuildSearchTaskAssignment(state)};
  }
  vector<DistributedScanSplit> result;
  if (state.execution_variant ==
      LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
    result.reserve(state.fragment_ids.size());
    for (auto fragment_id : state.fragment_ids) {
      result.push_back(
          BuildSearchTaskAssignment(state, optional_idx(fragment_id)));
    }
    return result;
  }
  if (state.execution_variant == LanceVaneSearchTaskVariant::FTS_CANDIDATES) {
    result.reserve(state.fts_segment_uuids.size());
    for (auto &segment_uuid : state.fts_segment_uuids) {
      result.push_back(
          BuildSearchTaskAssignment(state, optional_idx(), segment_uuid));
    }
    return result;
  }
  result.reserve(state.indexed_vector_segment_uuids.size() +
                 state.indexed_vector_uncovered_fragment_ids.size());
  for (auto &segment_uuid : state.indexed_vector_segment_uuids) {
    result.push_back(
        BuildSearchTaskAssignment(state, optional_idx(), segment_uuid));
  }
  for (auto fragment_id : state.indexed_vector_uncovered_fragment_ids) {
    result.push_back(
        BuildSearchTaskAssignment(state, optional_idx(fragment_id)));
  }
  return result;
}

void LanceVanePrepareSearchWorkerBindState(LanceVaneGlobalSearchState &state) {
  ValidateGlobalSearchState(state, true);
  if (!state.valid || !state.finalized) {
    throw InvalidInputException(
        "Distributed Lance search cannot prepare a contradictory worker bind");
  }
  // Vane may translate a serialized detached worker plan again before task
  // assignment. Its already-restricted bind is the portable source of truth;
  // preserving it also keeps an applied no-work assignment fail closed.
  if (state.worker_bind) {
    return;
  }
  if (state.task_assignment_applied || state.empty_assignment ||
      state.authorization_restricted || !state.authorized_task_ids.empty() ||
      !state.authorized_task_payloads.empty()) {
    throw InvalidInputException(
        "Distributed Lance search cannot prepare a contradictory worker bind");
  }

  auto assignments = LanceVaneCreateSearchTaskAssignments(state);
  state.worker_bind = true;
  state.authorization_restricted = true;
  state.authorized_task_ids.reserve(assignments.size());
  state.authorized_task_payloads.reserve(assignments.size());
  for (auto &assignment : assignments) {
    state.authorized_task_ids.push_back(assignment.split_id);
    state.authorized_task_payloads.push_back(assignment.payload);
  }
  ValidateGlobalSearchState(state, true);
}

void LanceVaneApplySearchTaskAssignments(
    LanceVaneGlobalSearchState &state,
    const vector<DistributedScanSplit> &splits) {
  ValidateGlobalSearchState(state, true);
  if (!state.worker_bind) {
    throw InvalidInputException(
        "Distributed Lance search task assignments require a detached worker "
        "bind");
  }
  if (splits.empty()) {
    // Vane removes the explicit EmptyExtension envelope before invoking the
    // callback. The physical scan is suppressed after this state is installed.
    if (state.task_assignment_applied) {
      if (!state.empty_assignment) {
        throw InvalidInputException(
            "Distributed Lance search retry changed its task assignment");
      }
      return;
    }
    state.authorized_task_ids.clear();
    state.authorized_task_payloads.clear();
    state.selected_fragment_ids.clear();
    state.selected_index_segment_uuids.clear();
    state.empty_assignment = true;
    state.task_assignment_applied = true;
    ValidateGlobalSearchState(state, true);
    return;
  }
  if (state.empty_assignment) {
    throw InvalidInputException(
        "Distributed Lance search retry changed its task assignment");
  }
  if (state.execution_variant == LanceVaneSearchTaskVariant::FINAL_SEARCH &&
      splits.size() != 1) {
    throw InvalidInputException(
        "Distributed Lance search requires exactly one preauthorized "
        "SearchTaskAssignment");
  }
  if (!state.authorization_restricted) {
    throw InvalidInputException(
        "Distributed Lance search task assignment was not preauthorized");
  }

  struct ValidatedSplit {
    string task_id;
    string payload;
    optional_idx fragment_id;
    string index_segment_uuid;
  };
  vector<ValidatedSplit> normalized;
  normalized.reserve(splits.size());
  for (auto &split : splits) {
    split.Validate();
    auto assignment =
        ValidateAuthorizedTask(state, split.split_id, split.payload);
    bool preauthorized = false;
    for (idx_t i = 0; i < state.authorized_task_ids.size(); i++) {
      if (state.authorized_task_ids[i] == split.split_id &&
          state.authorized_task_payloads[i] == split.payload) {
        preauthorized = true;
        break;
      }
    }
    if (!preauthorized) {
      throw InvalidInputException(
          "Distributed Lance search received an unauthorized task");
    }
    normalized.push_back({split.split_id, split.payload, assignment.fragment_id,
                          assignment.index_segment_uuid});
  }
  std::sort(normalized.begin(), normalized.end(),
            [](const ValidatedSplit &left, const ValidatedSplit &right) {
              if (!left.index_segment_uuid.empty() ||
                  !right.index_segment_uuid.empty()) {
                if (left.index_segment_uuid.empty()) {
                  return false;
                }
                if (right.index_segment_uuid.empty()) {
                  return true;
                }
                return left.index_segment_uuid < right.index_segment_uuid;
              }
              if (left.fragment_id.IsValid() && right.fragment_id.IsValid()) {
                return left.fragment_id.GetIndex() <
                       right.fragment_id.GetIndex();
              }
              return left.task_id < right.task_id;
            });
  for (idx_t i = 1; i < normalized.size(); i++) {
    if (normalized[i - 1].task_id == normalized[i].task_id) {
      throw InvalidInputException(
          "Distributed Lance search received a duplicate task assignment");
    }
  }
  vector<string> assigned_ids;
  vector<string> assigned_payloads;
  vector<uint64_t> selected_fragments;
  vector<string> selected_segments;
  assigned_ids.reserve(normalized.size());
  assigned_payloads.reserve(normalized.size());
  selected_fragments.reserve(normalized.size());
  selected_segments.reserve(normalized.size());
  for (auto &assignment : normalized) {
    assigned_ids.push_back(std::move(assignment.task_id));
    assigned_payloads.push_back(std::move(assignment.payload));
    if (assignment.fragment_id.IsValid()) {
      selected_fragments.push_back(assignment.fragment_id.GetIndex());
    }
    if (!assignment.index_segment_uuid.empty()) {
      selected_segments.push_back(std::move(assignment.index_segment_uuid));
    }
  }
  if (state.task_assignment_applied &&
      (state.authorized_task_ids != assigned_ids ||
       state.authorized_task_payloads != assigned_payloads ||
       state.selected_fragment_ids != selected_fragments ||
       state.selected_index_segment_uuids != selected_segments)) {
    throw InvalidInputException(
        "Distributed Lance search retry changed its task assignment");
  }
  state.authorized_task_ids = std::move(assigned_ids);
  state.authorized_task_payloads = std::move(assigned_payloads);
  state.selected_fragment_ids = std::move(selected_fragments);
  state.selected_index_segment_uuids = std::move(selected_segments);
  state.empty_assignment = false;
  state.task_assignment_applied = true;
  ValidateGlobalSearchState(state, true);
}

void LanceVaneSerializeGlobalSearchState(
    Serializer &serializer, const LanceVaneGlobalSearchState &state) {
  ValidateGlobalSearchState(state, true);
  serializer.WriteProperty(200, "valid", state.valid);
  serializer.WriteProperty(201, "contract_version", state.contract_version);
  serializer.WriteProperty(202, "source_class",
                           static_cast<uint64_t>(state.source_class));
  serializer.WriteProperty(203, "physical_uri", state.physical_uri);
  serializer.WriteProperty(204, "dataset_version", state.dataset_version);
  serializer.WriteProperty(205, "dataset_generation_id",
                           state.dataset_generation_id);
  serializer.WriteProperty(206, "search_node_uuid", state.search_node_uuid);
  serializer.WriteProperty(207, "private_uri_diagnostics",
                           state.private_uri_diagnostics);
  serializer.WriteProperty(208, "search_kind",
                           static_cast<uint64_t>(state.arguments.kind));
  serializer.WriteProperty(209, "search_overload",
                           static_cast<uint64_t>(state.arguments.overload));
  serializer.WriteProperty(210, "vector_column", state.arguments.vector_column);
  serializer.WriteProperty(211, "vector_query", state.arguments.vector_query);
  serializer.WriteProperty(212, "text_column", state.arguments.text_column);
  serializer.WriteProperty(213, "text_query", state.arguments.text_query);
  serializer.WriteProperty(214, "k", state.arguments.k);
  serializer.WriteProperty(215, "nprobes", state.arguments.nprobes);
  serializer.WriteProperty(216, "refine_factor", state.arguments.refine_factor);
  serializer.WriteProperty(217, "prefilter", state.arguments.prefilter);
  serializer.WriteProperty(218, "use_index", state.arguments.use_index);
  serializer.WriteProperty(219, "alpha", state.arguments.alpha);
  serializer.WriteProperty(220, "oversample_factor",
                           state.arguments.oversample_factor);
  serializer.WriteProperty(221, "explain_verbose",
                           state.arguments.explain_verbose);
  serializer.WriteProperty(222, "output_names", state.output_names);
  serializer.WriteProperty(223, "output_types", state.output_types);
  serializer.WriteProperty(224, "column_ids", state.column_ids);
  serializer.WriteProperty(225, "projection_ids", state.projection_ids);
  serializer.WriteProperty(226, "final_filter_ir", state.final_filter_ir);
  serializer.WriteProperty(227, "filter_fingerprint", state.filter_fingerprint);
  serializer.WriteProperty(228, "filter_pushed_down", state.filter_pushed_down);
  serializer.WriteProperty(229, "index_plan", state.index_plan);
  serializer.WriteProperty(230, "state_sha256", state.state_sha256);
  serializer.WriteProperty(231, "worker_bind", state.worker_bind);
  serializer.WriteProperty(232, "task_assignment_applied",
                           state.task_assignment_applied);
  serializer.WriteProperty(233, "empty_assignment", state.empty_assignment);
  serializer.WriteProperty(234, "authorization_restricted",
                           state.authorization_restricted);
  serializer.WriteProperty(235, "authorized_task_ids",
                           state.authorized_task_ids);
  serializer.WriteProperty(236, "authorized_task_payloads",
                           state.authorized_task_payloads);
  serializer.WriteProperty(237, "schema_fingerprint", state.schema_fingerprint);
  serializer.WriteProperty(238, "namespace_filter_plan",
                           state.namespace_filter_plan);
  serializer.WriteProperty(239, "finalized", state.finalized);
  serializer.WriteProperty(240, "qualification_failure",
                           state.qualification_failure);
  serializer.WriteProperty(241, "pending_filter_ir_parts",
                           state.pending_filter_ir_parts);
  serializer.WriteProperty(242, "pending_complex_filter_pushdown_failed",
                           state.pending_complex_filter_pushdown_failed);
  serializer.WriteProperty(243, "namespace_backed",
                           state.arguments.namespace_backed);
  auto has_frozen_snapshot = static_cast<bool>(state.frozen_snapshot);
  serializer.WriteProperty(244, "has_frozen_snapshot", has_frozen_snapshot);
  serializer.WriteProperty(245, "frozen_snapshot_version",
                           LANCE_VANE_FROZEN_SEARCH_SNAPSHOT_VERSION);
  serializer.WriteProperty(
      246, "serialized_manifest",
      has_frozen_snapshot ? state.frozen_snapshot->dataset.serialized_manifest
                          : string());
  serializer.WriteProperty(247, "manifest_sha256",
                           has_frozen_snapshot
                               ? state.frozen_snapshot->dataset.manifest_sha256
                               : string());
  serializer.WriteProperty(248, "serialized_index_section",
                           has_frozen_snapshot
                               ? state.frozen_snapshot->serialized_index_section
                               : string());
  serializer.WriteProperty(249, "index_section_sha256",
                           has_frozen_snapshot
                               ? state.frozen_snapshot->index_section_sha256
                               : string());
  serializer.WriteProperty(250, "execution_variant",
                           static_cast<uint64_t>(state.execution_variant));
  serializer.WriteProperty(251, "fragment_ids", state.fragment_ids);
  serializer.WriteProperty(252, "fragment_row_counts",
                           state.fragment_row_counts);
  serializer.WriteProperty(253, "fragment_bytes_on_disk",
                           state.fragment_bytes_on_disk);
  serializer.WriteProperty(254, "selected_fragment_ids",
                           state.selected_fragment_ids);
  serializer.WriteProperty(255, "indexed_vector_segment_uuids",
                           state.indexed_vector_segment_uuids);
  serializer.WriteProperty(256, "indexed_vector_segment_fragment_offsets",
                           state.indexed_vector_segment_fragment_offsets);
  serializer.WriteProperty(257, "indexed_vector_segment_fragment_ids",
                           state.indexed_vector_segment_fragment_ids);
  serializer.WriteProperty(258, "indexed_vector_uncovered_fragment_ids",
                           state.indexed_vector_uncovered_fragment_ids);
  serializer.WriteProperty(259, "selected_index_segment_uuids",
                           state.selected_index_segment_uuids);
  serializer.WriteProperty(260, "fts_segment_uuids", state.fts_segment_uuids);
  serializer.WriteProperty(261, "fts_segment_fragment_offsets",
                           state.fts_segment_fragment_offsets);
  serializer.WriteProperty(262, "fts_segment_fragment_ids",
                           state.fts_segment_fragment_ids);
}

LanceVaneGlobalSearchState
LanceVaneDeserializeGlobalSearchState(Deserializer &deserializer) {
  LanceVaneGlobalSearchState state;
  state.valid = deserializer.ReadProperty<bool>(200, "valid");
  state.contract_version =
      deserializer.ReadProperty<uint64_t>(201, "contract_version");
  auto source_class = deserializer.ReadProperty<uint64_t>(202, "source_class");
  if (source_class >
      static_cast<uint64_t>(LanceVaneSearchSourceClass::STANDARD_REST)) {
    throw SerializationException(
        "Distributed Lance search has an invalid source class");
  }
  state.source_class = static_cast<LanceVaneSearchSourceClass>(source_class);
  state.physical_uri = deserializer.ReadProperty<string>(203, "physical_uri");
  state.dataset_version =
      deserializer.ReadProperty<uint64_t>(204, "dataset_version");
  state.dataset_generation_id =
      deserializer.ReadProperty<string>(205, "dataset_generation_id");
  state.search_node_uuid =
      deserializer.ReadProperty<string>(206, "search_node_uuid");
  state.private_uri_diagnostics =
      deserializer.ReadProperty<bool>(207, "private_uri_diagnostics");
  auto kind = deserializer.ReadProperty<uint64_t>(208, "search_kind");
  auto overload = deserializer.ReadProperty<uint64_t>(209, "search_overload");
  if (kind > static_cast<uint64_t>(LanceVaneSearchKind::HYBRID) ||
      overload >
          static_cast<uint64_t>(LanceVaneSearchOverload::HYBRID_DOUBLE)) {
    throw SerializationException(
        "Distributed Lance search has an invalid function identity");
  }
  state.arguments.kind = static_cast<LanceVaneSearchKind>(kind);
  state.arguments.overload = static_cast<LanceVaneSearchOverload>(overload);
  state.arguments.vector_column =
      deserializer.ReadProperty<string>(210, "vector_column");
  state.arguments.vector_query =
      deserializer.ReadProperty<vector<float>>(211, "vector_query");
  state.arguments.text_column =
      deserializer.ReadProperty<string>(212, "text_column");
  state.arguments.text_query =
      deserializer.ReadProperty<string>(213, "text_query");
  state.arguments.k = deserializer.ReadProperty<uint64_t>(214, "k");
  state.arguments.nprobes = deserializer.ReadProperty<uint64_t>(215, "nprobes");
  state.arguments.refine_factor =
      deserializer.ReadProperty<uint64_t>(216, "refine_factor");
  state.arguments.prefilter = deserializer.ReadProperty<bool>(217, "prefilter");
  state.arguments.use_index = deserializer.ReadProperty<bool>(218, "use_index");
  state.arguments.alpha = deserializer.ReadProperty<float>(219, "alpha");
  state.arguments.oversample_factor =
      deserializer.ReadProperty<uint32_t>(220, "oversample_factor");
  state.arguments.explain_verbose =
      deserializer.ReadProperty<bool>(221, "explain_verbose");
  state.output_names =
      deserializer.ReadProperty<vector<string>>(222, "output_names");
  state.output_types =
      deserializer.ReadProperty<vector<LogicalType>>(223, "output_types");
  state.column_ids =
      deserializer.ReadProperty<vector<ColumnIndex>>(224, "column_ids");
  state.projection_ids =
      deserializer.ReadProperty<vector<idx_t>>(225, "projection_ids");
  state.final_filter_ir =
      deserializer.ReadProperty<string>(226, "final_filter_ir");
  state.filter_fingerprint =
      deserializer.ReadProperty<string>(227, "filter_fingerprint");
  state.filter_pushed_down =
      deserializer.ReadProperty<bool>(228, "filter_pushed_down");
  state.index_plan = deserializer.ReadProperty<string>(229, "index_plan");
  state.state_sha256 = deserializer.ReadProperty<string>(230, "state_sha256");
  state.worker_bind = deserializer.ReadProperty<bool>(231, "worker_bind");
  state.task_assignment_applied =
      deserializer.ReadProperty<bool>(232, "task_assignment_applied");
  state.empty_assignment =
      deserializer.ReadProperty<bool>(233, "empty_assignment");
  state.authorization_restricted =
      deserializer.ReadProperty<bool>(234, "authorization_restricted");
  state.authorized_task_ids =
      deserializer.ReadProperty<vector<string>>(235, "authorized_task_ids");
  state.authorized_task_payloads = deserializer.ReadProperty<vector<string>>(
      236, "authorized_task_payloads");
  state.schema_fingerprint =
      deserializer.ReadProperty<string>(237, "schema_fingerprint");
  state.namespace_filter_plan =
      deserializer.ReadProperty<string>(238, "namespace_filter_plan");
  state.finalized = deserializer.ReadProperty<bool>(239, "finalized");
  state.qualification_failure =
      deserializer.ReadProperty<string>(240, "qualification_failure");
  state.pending_filter_ir_parts =
      deserializer.ReadProperty<vector<string>>(241, "pending_filter_ir_parts");
  state.pending_complex_filter_pushdown_failed =
      deserializer.ReadProperty<bool>(242,
                                      "pending_complex_filter_pushdown_failed");
  state.arguments.namespace_backed =
      deserializer.ReadProperty<bool>(243, "namespace_backed");
  auto has_frozen_snapshot =
      deserializer.ReadProperty<bool>(244, "has_frozen_snapshot");
  auto frozen_snapshot_version =
      deserializer.ReadProperty<uint64_t>(245, "frozen_snapshot_version");
  auto serialized_manifest =
      deserializer.ReadProperty<string>(246, "serialized_manifest");
  auto manifest_sha256 =
      deserializer.ReadProperty<string>(247, "manifest_sha256");
  auto serialized_index_section =
      deserializer.ReadProperty<string>(248, "serialized_index_section");
  auto index_section_sha256 =
      deserializer.ReadProperty<string>(249, "index_section_sha256");
  auto execution_variant =
      deserializer.ReadProperty<uint64_t>(250, "execution_variant");
  if (execution_variant !=
          static_cast<uint64_t>(LanceVaneSearchTaskVariant::FINAL_SEARCH) &&
      execution_variant != static_cast<uint64_t>(
                               LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) &&
      execution_variant !=
          static_cast<uint64_t>(
              LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES) &&
      execution_variant !=
          static_cast<uint64_t>(LanceVaneSearchTaskVariant::FTS_CANDIDATES)) {
    throw SerializationException(
        "Distributed Lance search has an invalid execution variant");
  }
  state.execution_variant =
      static_cast<LanceVaneSearchTaskVariant>(execution_variant);
  state.fragment_ids =
      deserializer.ReadProperty<vector<uint64_t>>(251, "fragment_ids");
  state.fragment_row_counts =
      deserializer.ReadProperty<vector<int64_t>>(252, "fragment_row_counts");
  state.fragment_bytes_on_disk = deserializer.ReadProperty<vector<uint64_t>>(
      253, "fragment_bytes_on_disk");
  state.selected_fragment_ids =
      deserializer.ReadProperty<vector<uint64_t>>(254, "selected_fragment_ids");
  state.indexed_vector_segment_uuids =
      deserializer.ReadProperty<vector<string>>(255,
                                                "indexed_vector_segment_uuids");
  state.indexed_vector_segment_fragment_offsets =
      deserializer.ReadProperty<vector<uint64_t>>(
          256, "indexed_vector_segment_fragment_offsets");
  state.indexed_vector_segment_fragment_ids =
      deserializer.ReadProperty<vector<uint64_t>>(
          257, "indexed_vector_segment_fragment_ids");
  state.indexed_vector_uncovered_fragment_ids =
      deserializer.ReadProperty<vector<uint64_t>>(
          258, "indexed_vector_uncovered_fragment_ids");
  state.selected_index_segment_uuids =
      deserializer.ReadProperty<vector<string>>(259,
                                                "selected_index_segment_uuids");
  state.fts_segment_uuids =
      deserializer.ReadProperty<vector<string>>(260, "fts_segment_uuids");
  state.fts_segment_fragment_offsets =
      deserializer.ReadProperty<vector<uint64_t>>(
          261, "fts_segment_fragment_offsets");
  state.fts_segment_fragment_ids = deserializer.ReadProperty<vector<uint64_t>>(
      262, "fts_segment_fragment_ids");
  if (frozen_snapshot_version != LANCE_VANE_FROZEN_SEARCH_SNAPSHOT_VERSION) {
    throw SerializationException(
        "Distributed Lance search has an unsupported frozen snapshot format");
  }
  if (has_frozen_snapshot) {
    auto frozen_snapshot = make_shared_ptr<LanceVaneFrozenSearchSnapshot>();
    frozen_snapshot->dataset.serialized_manifest =
        std::move(serialized_manifest);
    frozen_snapshot->dataset.manifest_sha256 = std::move(manifest_sha256);
    frozen_snapshot->dataset.schema_fingerprint = state.schema_fingerprint;
    frozen_snapshot->serialized_index_section =
        std::move(serialized_index_section);
    frozen_snapshot->index_section_sha256 = std::move(index_section_sha256);
    state.frozen_snapshot = std::move(frozen_snapshot);
    ValidateAndMarkFrozenSearchSnapshot(state);
  } else if (!serialized_manifest.empty() || !manifest_sha256.empty() ||
             !serialized_index_section.empty() ||
             !index_section_sha256.empty()) {
    throw SerializationException(
        "Distributed Lance search has an unclaimed frozen snapshot payload");
  }
  if (state.valid) {
    ValidateAndMarkSearchPlanPayloads(state);
  }
  ValidateGlobalSearchState(state, true);
  return state;
}

shared_ptr<LanceDatasetCacheEntry>
OpenValidatedSearchSnapshot(ClientContext &context,
                            const LanceVaneGlobalSearchState &state) {
  auto replay_path = LanceVaneReplayPath(context, state.physical_uri);
  if (replay_path != state.physical_uri) {
    throw IOException(
        "Distributed Lance search physical identity changed before execution");
  }

  auto &frozen = *state.frozen_snapshot;
  return LanceVaneGetOrOpenFrozenSearchSnapshot(
      context, state.physical_uri, state.dataset_version,
      state.dataset_generation_id, frozen.dataset.serialized_manifest,
      frozen.dataset.manifest_sha256, frozen.serialized_index_section,
      frozen.index_section_sha256, state.schema_fingerprint,
      state.private_uri_diagnostics);
}

shared_ptr<LanceDatasetCacheEntry>
LanceVaneOpenSearchSnapshot(ClientContext &context,
                            const LanceVaneGlobalSearchState &state) {
  ValidateGlobalSearchState(state, true);
  if (!state.worker_bind || !state.task_assignment_applied ||
      state.empty_assignment ||
      (state.execution_variant ==
           LanceVaneSearchTaskVariant::VECTOR_CANDIDATES &&
       state.selected_fragment_ids.empty()) ||
      (state.execution_variant ==
           LanceVaneSearchTaskVariant::INDEXED_VECTOR_CANDIDATES &&
       state.selected_fragment_ids.empty() &&
       state.selected_index_segment_uuids.empty()) ||
      (state.execution_variant == LanceVaneSearchTaskVariant::FTS_CANDIDATES &&
       state.selected_index_segment_uuids.empty())) {
    throw InvalidInputException(
        "Distributed Lance search execution requires authorized task "
        "assignments");
  }
  return OpenValidatedSearchSnapshot(context, state);
}

shared_ptr<LanceDatasetCacheEntry>
LanceVaneOpenSearchSnapshotForMaterialization(
    ClientContext &context, const LanceVaneGlobalSearchState &state) {
  ValidateGlobalSearchState(state, true);
  if (!state.valid || !state.finalized || state.worker_bind ||
      !LanceVaneIsCandidateVariant(state.execution_variant)) {
    throw InvalidInputException(
        "Distributed Lance search materialization state is contradictory");
  }
  return OpenValidatedSearchSnapshot(context, state);
}

void LanceVanePopulateSearchSchema(ClientContext &context,
                                   const vector<string> &names,
                                   const vector<LogicalType> &types,
                                   ArrowSchemaWrapper &schema_root,
                                   ArrowTableSchema &arrow_table) {
  if (names.empty() || names.size() != types.size()) {
    throw SerializationException(
        "Distributed Lance search has an invalid output schema");
  }
  std::memset(&schema_root.arrow_schema, 0, sizeof(schema_root.arrow_schema));
  auto properties = context.GetClientProperties();
  ArrowConverter::ToArrowSchema(&schema_root.arrow_schema, types, names,
                                properties);
  LanceCoerceArrowSchemaForDuckDB(&schema_root.arrow_schema);
  ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table,
                                               schema_root.arrow_schema);
}

void LanceVaneValidateExecutionInput(const TableFunctionInitInput &input,
                                     const LanceVaneGlobalSearchState &state) {
  ValidateGlobalSearchState(state, true);
  if (LanceVaneIsCandidateVariant(state.execution_variant)) {
    if (input.column_indexes !=
            vector<ColumnIndex>{ColumnIndex(0), ColumnIndex(1)} ||
        !input.projection_ids.empty() ||
        (input.filters && !input.filters->filters.empty())) {
      throw InvalidInputException(
          "Distributed Lance search candidate projection changed after "
          "admission");
    }
    return;
  }
  if (input.column_indexes != state.column_ids ||
      input.projection_ids != state.projection_ids) {
    throw InvalidInputException(
        "Distributed Lance search projection changed after admission");
  }
  auto fingerprint = BuildFilterFingerprint(
      state.output_names, input.column_indexes,
      optional_ptr<const TableFilterSet>(
          static_cast<const TableFilterSet *>(input.filters.get())));
  if (fingerprint != state.filter_fingerprint) {
    throw InvalidInputException(
        "Distributed Lance search filters changed after admission");
  }
}

void LanceVaneValidateDistributedInput(
    const TableFunctionDistributedScanInput &input,
    const LanceVaneGlobalSearchState &state) {
  ValidateGlobalSearchState(state, true);
  if (LanceVaneIsCandidateVariant(state.execution_variant)) {
    if (input.column_ids !=
            vector<ColumnIndex>{ColumnIndex(0), ColumnIndex(1)} ||
        !input.projection_ids.empty() ||
        (input.table_filters && !input.table_filters->filters.empty())) {
      throw InvalidInputException(
          "Distributed Lance search candidate projection changed after "
          "admission");
    }
    return;
  }
  if (input.column_ids != state.column_ids ||
      input.projection_ids != state.projection_ids) {
    throw InvalidInputException(
        "Distributed Lance search projection changed after admission");
  }
  auto fingerprint = BuildFilterFingerprint(
      state.output_names, input.column_ids, input.table_filters);
  if (fingerprint != state.filter_fingerprint) {
    throw InvalidInputException(
        "Distributed Lance search filters changed after admission");
  }
}

TableFunctionDistributedScanCallbacks LanceVaneSearchTaskCallbacks(
    table_function_plan_distributed_scan_splits_t plan_splits,
    table_function_create_distributed_worker_bind_t create_worker_bind,
    table_function_apply_distributed_scan_splits_t apply_splits) {
  TableFunctionDistributedScanCallbacks callbacks;
  callbacks.protocol_version = LANCE_VANE_SEARCH_PROTOCOL_VERSION;
  callbacks.split_codec = {LANCE_VANE_SEARCH_SPLIT_CODEC,
                           LANCE_VANE_SEARCH_SPLIT_CODEC_VERSION};
  callbacks.plan_splits = plan_splits;
  callbacks.create_worker_bind = create_worker_bind;
  callbacks.apply_splits = apply_splits;
  return callbacks;
}

} // namespace duckdb

#endif
