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
    "lance.global-search-split";
static constexpr const char *LANCE_VANE_SEARCH_STATE_MAGIC = "LVS1";
static constexpr const char *LANCE_VANE_GLOBAL_SEARCH_MAGIC = "LGS1";
static constexpr idx_t LANCE_VANE_GLOBAL_SEARCH_MAGIC_SIZE = 4;
static constexpr idx_t LANCE_VANE_SEARCH_UUID_SIZE = BaseUUID::STRING_SIZE;
static constexpr idx_t LANCE_VANE_SHA256_SIZE = 32;
static constexpr uint16_t LANCE_VANE_GLOBAL_SEARCH_FORMAT_VERSION = 1;

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
  writer.String(LANCE_VANE_SEARCH_STATE_MAGIC);
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

static string
ExpectedGlobalSearchSplitPayload(const LanceVaneGlobalSearchState &state) {
  string payload(LANCE_VANE_GLOBAL_SEARCH_MAGIC,
                 LANCE_VANE_GLOBAL_SEARCH_MAGIC_SIZE);
  payload.push_back(
      static_cast<char>(LANCE_VANE_GLOBAL_SEARCH_FORMAT_VERSION & 0xff));
  payload.push_back(
      static_cast<char>((LANCE_VANE_GLOBAL_SEARCH_FORMAT_VERSION >> 8) & 0xff));
  payload.append(state.search_node_uuid);
  payload.append(state.state_sha256);
  return payload;
}

static void ValidateGlobalSearchState(const LanceVaneGlobalSearchState &state,
                                      bool verify_digest) {
  if (state.contract_version != LANCE_VANE_SEARCH_CONTRACT_VERSION ||
      static_cast<uint8_t>(state.source_class) >
          static_cast<uint8_t>(LanceVaneSearchSourceClass::STANDARD_REST) ||
      !IsValidSearchUUID(state.search_node_uuid) ||
      state.output_names.empty() ||
      state.output_names.size() != state.output_types.size() ||
      state.state_sha256.size() != LANCE_VANE_SHA256_SIZE ||
      state.authorized_split_ids.size() !=
          state.authorized_split_payloads.size()) {
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
        !state.column_ids.empty() || !state.projection_ids.empty() ||
        !state.final_filter_ir.empty() || !state.filter_fingerprint.empty() ||
        state.filter_pushed_down || state.worker_bind || state.splits_applied ||
        state.empty_assignment || state.authorization_restricted ||
        !state.authorized_split_ids.empty() || state.frozen_snapshot ||
        state.frozen_snapshot_payload_validated) {
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
      !state.frozen_snapshot || !state.frozen_snapshot_payload_validated ||
      state.frozen_snapshot->dataset.schema_fingerprint !=
          state.schema_fingerprint ||
      state.index_plan.size() < 6 ||
      state.index_plan.compare(0, 4, "LSI1", 4) != 0 ||
      (!state.namespace_filter_plan.empty() &&
       (state.namespace_filter_plan.size() <= 6 ||
        state.namespace_filter_plan.compare(0, 4, "LNF1", 4) != 0))) {
    throw SerializationException("Distributed Lance search state is malformed");
  }
  if (!state.finalized &&
      (!state.column_ids.empty() || !state.projection_ids.empty() ||
       !state.final_filter_ir.empty() || state.filter_pushed_down ||
       state.worker_bind || state.splits_applied || state.empty_assignment ||
       state.authorization_restricted || !state.authorized_split_ids.empty())) {
    throw SerializationException(
        "Distributed Lance search coordinator state is contradictory");
  }
  if (state.finalized && (!state.pending_filter_ir_parts.empty() ||
                          state.pending_complex_filter_pushdown_failed)) {
    throw SerializationException(
        "Distributed Lance search finalized state retains planning inputs");
  }
  if ((!state.worker_bind && (state.splits_applied || state.empty_assignment ||
                              state.authorization_restricted ||
                              !state.authorized_split_ids.empty())) ||
      (!state.authorization_restricted &&
       (state.splits_applied || state.empty_assignment ||
        !state.authorized_split_ids.empty())) ||
      (state.authorization_restricted &&
       (!state.worker_bind || !state.splits_applied ||
        (state.empty_assignment && !state.authorized_split_ids.empty()) ||
        (!state.empty_assignment &&
         (state.authorized_split_ids.size() != 1 ||
          state.authorized_split_ids[0] != "global:" + state.search_node_uuid ||
          state.authorized_split_payloads[0] !=
              ExpectedGlobalSearchSplitPayload(state)))))) {
    throw SerializationException(
        "Distributed Lance search assignment state is contradictory");
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
  if (rc != 0 || !bytes || len <= 6) {
    if (bytes) {
      lance_vane_free_bytes(bytes, len);
    }
    throw InvalidInputException(
        "Distributed Lance search cannot reproduce the namespace filter "
        "against the frozen physical schema");
  }
  string result(reinterpret_cast<const char *>(bytes), len);
  lance_vane_free_bytes(bytes, len);
  if (result.compare(0, 4, "LNF1", 4) != 0) {
    throw InternalException(
        "Distributed Lance namespace filter planner returned an invalid plan");
  }
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

static string EncodeGlobalSearchSplit(const LanceVaneGlobalSearchState &state) {
  ValidateGlobalSearchState(state, true);
  return ExpectedGlobalSearchSplitPayload(state);
}

static void ValidateGlobalSearchSplit(const LanceVaneGlobalSearchState &state,
                                      const DistributedScanSplit &split) {
  split.Validate();
  auto expected_id = "global:" + state.search_node_uuid;
  if (split.split_id != expected_id ||
      split.payload != EncodeGlobalSearchSplit(state)) {
    throw SerializationException(
        "Distributed Lance search received an unauthorized global split");
  }
}

DistributedScanSplit
LanceVaneCreateGlobalSearchSplit(const LanceVaneGlobalSearchState &state) {
  if (!state.finalized) {
    throw InvalidInputException(
        "Distributed Lance search was not finalized before split planning");
  }
  DistributedScanSplit split;
  split.split_id = "global:" + state.search_node_uuid;
  split.payload = EncodeGlobalSearchSplit(state);
  split.estimated_cardinality =
      optional_idx(NumericCast<idx_t>(state.arguments.k));
  split.Validate();
  return split;
}

void LanceVaneApplyGlobalSearchSplits(
    LanceVaneGlobalSearchState &state,
    const vector<DistributedScanSplit> &splits) {
  if (!state.worker_bind) {
    throw InvalidInputException(
        "Distributed Lance search splits require a detached worker bind");
  }
  if (splits.empty()) {
    if (state.authorization_restricted &&
        (!state.authorized_split_ids.empty() ||
         !state.authorized_split_payloads.empty())) {
      throw InvalidInputException(
          "Distributed Lance search retry changed its split assignment");
    }
    state.authorization_restricted = true;
    state.authorized_split_ids.clear();
    state.authorized_split_payloads.clear();
    state.empty_assignment = true;
    state.splits_applied = true;
    return;
  }
  if (splits.size() != 1) {
    throw InvalidInputException(
        "Distributed Lance search requires exactly one global split");
  }
  ValidateGlobalSearchSplit(state, splits[0]);
  if (state.authorization_restricted) {
    if (state.authorized_split_ids != vector<string>{splits[0].split_id} ||
        state.authorized_split_payloads != vector<string>{splits[0].payload}) {
      throw InvalidInputException(
          "Distributed Lance search retry changed its split assignment");
    }
  } else {
    state.authorization_restricted = true;
    state.authorized_split_ids = {splits[0].split_id};
    state.authorized_split_payloads = {splits[0].payload};
  }
  state.empty_assignment = false;
  state.splits_applied = true;
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
  serializer.WriteProperty(232, "splits_applied", state.splits_applied);
  serializer.WriteProperty(233, "empty_assignment", state.empty_assignment);
  serializer.WriteProperty(234, "authorization_restricted",
                           state.authorization_restricted);
  serializer.WriteProperty(235, "authorized_split_ids",
                           state.authorized_split_ids);
  serializer.WriteProperty(236, "authorized_split_payloads",
                           state.authorized_split_payloads);
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
  state.splits_applied = deserializer.ReadProperty<bool>(232, "splits_applied");
  state.empty_assignment =
      deserializer.ReadProperty<bool>(233, "empty_assignment");
  state.authorization_restricted =
      deserializer.ReadProperty<bool>(234, "authorization_restricted");
  state.authorized_split_ids =
      deserializer.ReadProperty<vector<string>>(235, "authorized_split_ids");
  state.authorized_split_payloads = deserializer.ReadProperty<vector<string>>(
      236, "authorized_split_payloads");
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
  ValidateGlobalSearchState(state, true);
  return state;
}

shared_ptr<LanceDatasetCacheEntry>
LanceVaneOpenSearchSnapshot(ClientContext &context,
                            const LanceVaneGlobalSearchState &state) {
  ValidateGlobalSearchState(state, true);
  if (!state.worker_bind || !state.splits_applied || state.empty_assignment) {
    throw InvalidInputException(
        "Distributed Lance search execution requires one authorized global "
        "split");
  }
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

TableFunctionDistributedScanCallbacks LanceVaneGlobalSearchCallbacks(
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
