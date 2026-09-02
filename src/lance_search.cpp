#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#ifdef LANCE_VANE_DISTRIBUTED
#include "duckdb/execution/external_block.hpp"
#include "duckdb/function/distributed_table_function.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#endif
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "lance_arrow_compat.hpp"
#include "lance_common.hpp"
#include "lance_dataset_cache.hpp"
#include "lance_ffi.hpp"
#include "lance_filter_ir.hpp"
#include "lance_resolver.hpp"
#include "lance_table_entry.hpp"
#ifdef LANCE_VANE_DISTRIBUTED
#include "lance_vane_rest_resolution.hpp"
#include "lance_vane_search.hpp"
#endif

#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <unordered_map>
#ifdef LANCE_VANE_DISTRIBUTED
#include <unordered_set>
#endif

namespace duckdb {

static bool TryLanceExplainKnn(void *dataset, const string &vector_column,
                               const vector<float> &query, uint64_t k,
                               uint64_t nprobes, uint64_t refine_factor,
                               const string *filter_ir, bool prefilter,
                               bool use_index, bool verbose, string &out_plan,
                               string &out_error) {
  out_plan.clear();
  out_error.clear();

  if (!dataset) {
    out_error = "dataset is null";
    return false;
  }
  if (query.empty()) {
    out_error = "query is empty";
    return false;
  }

  const uint8_t *filter_ptr = nullptr;
  idx_t filter_len = 0;
  if (filter_ir && !filter_ir->empty()) {
    filter_ptr = reinterpret_cast<const uint8_t *>(filter_ir->data());
    filter_len = NumericCast<idx_t>(filter_ir->size());
  }

  auto *plan_ptr = lance_explain_knn_scan_ir(
      dataset, vector_column.c_str(), query.data(), query.size(), k, nprobes,
      refine_factor, filter_ptr, NumericCast<size_t>(filter_len),
      prefilter ? 1 : 0, use_index ? 1 : 0, verbose ? 1 : 0);
  if (!plan_ptr) {
    out_error = LanceConsumeLastError();
    if (out_error.empty()) {
      out_error = "unknown error";
    }
    return false;
  }

  out_plan = plan_ptr;
  lance_free_string(plan_ptr);
  return true;
}

// TryResolveLanceTableEntry() is now defined in lance_common.cpp so that
// the maintenance helpers (compact / cleanup / optimize_index /
// auto_cleanup) can share the same "catalog.schema.table" -> entry
// resolution logic.

static shared_ptr<LanceDatasetCacheEntry>
OpenSearchDatasetEntry(ClientContext &context, const Value &input,
                       const string &function_name, string &out_display_uri,
                       bool *out_cache_hit) {
  out_display_uri = ResolveLanceDatasetUri(
      context, input, LanceResolvePolicy::FALLBACK_TO_PATH, function_name);
  auto input_str = input.GetValue<string>();
  if (auto *table = TryResolveLanceTableEntry(context, input_str)) {
    if (StringUtil::CIEquals(out_display_uri, table->DatasetUri())) {
      return LanceGetOrOpenDatasetEntryForTable(context, *table,
                                                out_display_uri, out_cache_hit);
    }
  }

  auto entry =
      LanceGetOrOpenDatasetEntry(context, out_display_uri, out_cache_hit);
  if (entry) {
    out_display_uri = entry->DisplayUri();
  }
  return entry;
}

static vector<float> ParseQueryVector(const Value &value,
                                      const string &function_name) {
  if (value.IsNull()) {
    throw InvalidInputException(function_name +
                                " requires a non-null query vector");
  }
  if (value.type().id() != LogicalTypeId::LIST &&
      value.type().id() != LogicalTypeId::ARRAY) {
    throw InvalidInputException(function_name +
                                " requires query vector to be a LIST or ARRAY");
  }
  vector<Value> children;
  if (value.type().id() == LogicalTypeId::LIST) {
    children = ListValue::GetChildren(value);
  } else {
    children = ArrayValue::GetChildren(value);
  }
  if (children.empty()) {
    throw InvalidInputException(function_name +
                                " requires a non-empty query vector");
  }

  auto cast_f32 = [&function_name](double v) {
    if (!std::isfinite(v)) {
      throw InvalidInputException(function_name +
                                  " query vector contains non-finite value");
    }
    auto max_v = static_cast<double>(std::numeric_limits<float>::max());
    if (v > max_v || v < -max_v) {
      throw InvalidInputException(
          function_name + " query vector value is out of float32 range");
    }
    return static_cast<float>(v);
  };

  vector<float> out;
  out.reserve(children.size());
  for (auto &child : children) {
    if (child.IsNull()) {
      throw InvalidInputException(function_name +
                                  " query vector contains NULL");
    }
    switch (child.type().id()) {
    case LogicalTypeId::FLOAT:
      out.push_back(cast_f32(child.GetValue<float>()));
      break;
    case LogicalTypeId::DOUBLE:
      out.push_back(cast_f32(child.GetValue<double>()));
      break;
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
      out.push_back(cast_f32(static_cast<double>(child.GetValue<int64_t>())));
      break;
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT:
      out.push_back(cast_f32(static_cast<double>(child.GetValue<uint64_t>())));
      break;
    default:
      try {
        auto dbl = child.DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
        out.push_back(cast_f32(dbl));
      } catch (Exception &) {
        throw InvalidInputException(function_name +
                                    " query vector elements must be numeric");
      }
    }
  }
  return out;
}

static string ParseOptionalNamedString(const TableFunctionBindInput &input,
                                       const string &name) {
  auto it = input.named_parameters.find(name);
  if (it == input.named_parameters.end() || it->second.IsNull()) {
    return string();
  }
  return it->second.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
}

static LanceTableEntry *
TryResolveNamespaceBackedSearchTable(ClientContext &context,
                                     const Value &input) {
  auto input_str = input.GetValue<string>();
  auto *table = TryResolveLanceTableEntry(context, input_str);
  if (!table || !table->IsNamespaceBacked()) {
    return nullptr;
  }
  return table;
}

static string RequireNamespaceSearchColumn(const LanceTableEntry &table,
                                           const string &column,
                                           const string &function_name,
                                           const string &argument_name) {
  for (auto &col : table.GetColumns().Physical()) {
    if (StringUtil::CIEquals(col.Name(), column)) {
      return col.Name();
    }
  }
  throw InvalidInputException(function_name + " requires " + argument_name +
                              " to name an existing column on " +
                              "namespace-backed table: " + column);
}

static void PopulateNamespaceSearchSchema(
    ClientContext &context, const LanceTableEntry &table,
    const string &metric_name, ArrowSchemaWrapper &schema_root,
    ArrowTableSchema &arrow_table, vector<string> &result_names,
    vector<LogicalType> &result_types, vector<string> &names,
    vector<LogicalType> &return_types) {
  vector<string> field_names;
  vector<LogicalType> field_types;
  field_names.reserve(table.GetColumns().PhysicalColumnCount() + 1);
  field_types.reserve(table.GetColumns().PhysicalColumnCount() + 1);
  for (auto &col : table.GetColumns().Physical()) {
    field_names.push_back(col.Name());
    field_types.push_back(col.Type());
  }
  field_names.push_back(metric_name);
  field_types.push_back(LogicalType::FLOAT);

  memset(&schema_root.arrow_schema, 0, sizeof(schema_root.arrow_schema));
  auto props = context.GetClientProperties();
  ArrowConverter::ToArrowSchema(&schema_root.arrow_schema, field_types,
                                field_names, props);
  LanceCoerceArrowSchemaForDuckDB(&schema_root.arrow_schema);
  ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table,
                                               schema_root.arrow_schema);
  result_names = arrow_table.GetNames();
  result_types = arrow_table.GetTypes();
  names = result_names;
  return_types = result_types;
}

#ifdef LANCE_VANE_DISTRIBUTED
enum class LanceVaneSearchColumnReferenceClass : uint8_t {
  NONE = 0,
  BASE_ONLY = 1,
  COMPUTED_ONLY = 2,
  MIXED_OR_UNRESOLVED = 3
};

static LanceVaneSearchColumnReferenceClass
ClassifyVaneSearchColumnReferences(const LogicalGet &get,
                                   const vector<string> &names,
                                   const Expression &expression) {
  bool saw_base = false;
  bool saw_computed = false;
  bool unresolved = false;
  auto classify_scan_index = [&](idx_t scan_index) {
    auto &column_ids = get.GetColumnIds();
    if (scan_index >= column_ids.size()) {
      unresolved = true;
      return;
    }
    auto &column_index = column_ids[scan_index];
    if (column_index.IsVirtualColumn() || !column_index.HasPrimaryIndex()) {
      unresolved = true;
      return;
    }
    auto column_id = column_index.GetPrimaryIndex();
    if (column_id >= names.size()) {
      unresolved = true;
      return;
    }
    if (IsComputedSearchColumn(names[column_id])) {
      saw_computed = true;
    } else {
      saw_base = true;
    }
  };

  ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
      expression, [&](const BoundColumnRefExpression &column_ref) {
        if (column_ref.depth != 0 ||
            column_ref.binding.table_index != get.table_index) {
          unresolved = true;
          return;
        }
        classify_scan_index(column_ref.binding.column_index);
      });
  ExpressionIterator::VisitExpression<BoundReferenceExpression>(
      expression, [&](const BoundReferenceExpression &reference) {
        classify_scan_index(reference.index);
      });

  if (unresolved || (saw_base && saw_computed)) {
    return LanceVaneSearchColumnReferenceClass::MIXED_OR_UNRESOLVED;
  }
  if (saw_computed) {
    return LanceVaneSearchColumnReferenceClass::COMPUTED_ONLY;
  }
  if (saw_base) {
    return LanceVaneSearchColumnReferenceClass::BASE_ONLY;
  }
  return LanceVaneSearchColumnReferenceClass::NONE;
}

static void PrepareVaneSearchCandidate(
    ClientContext &context, const Value &source_value,
    const shared_ptr<LanceDatasetCacheEntry> &dataset_entry,
    const string &physical_path, bool private_uri_diagnostics,
    LanceVanePhysicalCandidate &out_candidate) {
  try {
    auto *table =
        TryResolveLanceTableEntry(context, source_value.GetValue<string>());
    if (table && table->IsNamespaceBacked() &&
        table->NamespaceConfig().IsRest()) {
      LanceVaneResolveRestPhysicalCandidate(context, *table, dataset_entry,
                                            out_candidate);
      return;
    }
    auto source_class = table && table->IsNamespaceBacked()
                            ? LanceVaneSearchSourceClass::DIRECTORY_NAMESPACE
                            : LanceVaneSearchSourceClass::DIRECT;
    if (source_class == LanceVaneSearchSourceClass::DIRECTORY_NAMESPACE &&
        !LanceVaneDirectoryNamespaceSessionMatches(context,
                                                   table->NamespaceConfig())) {
      out_candidate = {};
      out_candidate.attempted = true;
      out_candidate.source_class = source_class;
      out_candidate.private_uri_diagnostics = private_uri_diagnostics;
      out_candidate.safe_failure =
          "Distributed Lance directory namespace searches require the query "
          "session storage settings to match the settings captured by ATTACH";
      return;
    }
    auto requires_secret =
        table && table->IsNamespaceBacked()
            ? table->NamespaceConfig().uses_coordinator_storage_secret
            : LanceHasMatchingStorageSecret(context, physical_path);
    LanceVaneCapturePhysicalCandidate(context, physical_path, dataset_entry,
                                      source_class, private_uri_diagnostics,
                                      requires_secret, out_candidate);
  } catch (Exception &) {
    out_candidate = {};
    out_candidate.attempted = true;
    out_candidate.safe_failure =
        "Distributed Lance search could not freeze a replayable physical "
        "candidate";
  }
}

static LanceVaneSearchOverload VaneVectorOverload(const LogicalType &query_type,
                                                  bool hybrid) {
  auto child_type = ListType::GetChildType(query_type);
  if (hybrid) {
    return child_type.id() == LogicalTypeId::DOUBLE
               ? LanceVaneSearchOverload::HYBRID_DOUBLE
               : LanceVaneSearchOverload::HYBRID_FLOAT;
  }
  return child_type.id() == LogicalTypeId::DOUBLE
             ? LanceVaneSearchOverload::VECTOR_DOUBLE
             : LanceVaneSearchOverload::VECTOR_FLOAT;
}
#endif

struct LanceKnnBindData : public TableFunctionData {
  string file_path;
#ifdef LANCE_VANE_DISTRIBUTED
  bool private_uri_diagnostics = false;
#endif
  string vector_column;
  vector<float> query;
  uint64_t k = 0;
  uint64_t nprobes = 0;
  uint64_t refine_factor = 0;
  bool prefilter = true;
  bool use_index = true;
  bool explain_verbose = false;
  bool namespace_backed = false;
  LanceNamespaceTableConfig namespace_config;
  string namespace_filter;

  shared_ptr<LanceDatasetCacheEntry> dataset_entry;
  void *dataset = nullptr;
  bool dataset_cache_hit = false;
  ArrowSchemaWrapper schema_root;
  ArrowTableSchema arrow_table;
  vector<string> names;
  vector<LogicalType> types;

  vector<string> lance_pushed_filter_ir_parts;
#ifdef LANCE_VANE_DISTRIBUTED
  bool complex_filter_pushdown_failed = false;
  LanceVaneSearchOverload vane_overload = LanceVaneSearchOverload::VECTOR_FLOAT;
  LanceVanePhysicalCandidate vane_candidate;
  LanceVaneGlobalSearchState vane_state;

  unique_ptr<FunctionData> Copy() const override {
    auto result = make_uniq<LanceKnnBindData>();
    result->column_ids = column_ids;
    result->file_path = file_path;
    result->private_uri_diagnostics = private_uri_diagnostics;
    result->vector_column = vector_column;
    result->query = query;
    result->k = k;
    result->nprobes = nprobes;
    result->refine_factor = refine_factor;
    result->prefilter = prefilter;
    result->use_index = use_index;
    result->explain_verbose = explain_verbose;
    result->namespace_backed = namespace_backed;
    result->namespace_config = namespace_config;
    result->namespace_filter = namespace_filter;
    result->dataset_entry = dataset_entry;
    result->dataset = dataset;
    result->dataset_cache_hit = dataset_cache_hit;
    result->arrow_table = arrow_table;
    result->names = names;
    result->types = types;
    result->lance_pushed_filter_ir_parts = lance_pushed_filter_ir_parts;
    result->complex_filter_pushdown_failed = complex_filter_pushdown_failed;
    result->vane_overload = vane_overload;
    result->vane_candidate = vane_candidate;
    result->vane_state = vane_state;
    return result;
  }
#endif
};

#ifdef LANCE_VANE_DISTRIBUTED
static void LancePrepareKnnVaneState(LanceKnnBindData &bind_data);
#endif

struct LanceKnnGlobalState : public GlobalTableFunctionState {
  std::atomic<idx_t> lines_read{0};
  std::atomic<idx_t> record_batches{0};
  std::atomic<idx_t> record_batch_rows{0};
  string lance_filter_ir;
  bool filter_pushed_down = false;
  std::atomic<idx_t> filter_pushdown_fallbacks{0};

  vector<idx_t> projection_ids;
  vector<LogicalType> scanned_types;
  vector<string> namespace_columns;

  std::atomic<bool> explain_computed{false};
  string explain_plan;
  string explain_error;
  std::mutex explain_mutex;

#ifdef LANCE_VANE_DISTRIBUTED
  shared_ptr<LanceDatasetCacheEntry> vane_dataset_entry;
  void *vane_dataset = nullptr;
#endif

  idx_t MaxThreads() const override { return 1; }
  bool CanRemoveFilterColumns() const { return !projection_ids.empty(); }
};

struct LanceKnnLocalState : public ArrowScanLocalState {
  explicit LanceKnnLocalState(unique_ptr<ArrowArrayWrapper> current_chunk,
                              ClientContext &context)
      : ArrowScanLocalState(std::move(current_chunk), context),
        filter_sel(STANDARD_VECTOR_SIZE) {}

  void *stream = nullptr;
  LanceKnnGlobalState *global_state = nullptr;
  bool filter_pushed_down = false;
  SelectionVector filter_sel;

  ~LanceKnnLocalState() override {
    if (stream) {
      lance_close_stream(stream);
    }
  }
};

static void
LancePushdownComplexFilter(ClientContext &, LogicalGet &get,
                           FunctionData *bind_data,
                           vector<unique_ptr<Expression>> &filters) {
  if (!bind_data || filters.empty()) {
    return;
  }
  auto &scan_bind = bind_data->Cast<LanceKnnBindData>();
  if (scan_bind.namespace_backed) {
    return;
  }

  for (auto &expr : filters) {
#ifdef LANCE_VANE_DISTRIBUTED
    if (expr &&
        ClassifyVaneSearchColumnReferences(get, scan_bind.names, *expr) ==
            LanceVaneSearchColumnReferenceClass::COMPUTED_ONLY) {
      continue;
    }
#endif
    if (!expr || expr->HasParameter() || expr->IsVolatile()) {
#ifdef LANCE_VANE_DISTRIBUTED
      scan_bind.complex_filter_pushdown_failed = true;
#endif
      continue;
    }
    if (expr->expression_class == ExpressionClass::BOUND_COMPARISON) {
      auto &cmp = expr->Cast<BoundComparisonExpression>();
      if (cmp.type == ExpressionType::COMPARE_DISTINCT_FROM ||
          cmp.type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
        auto is_constant = [](const unique_ptr<Expression> &node) -> bool {
          if (!node) {
            return false;
          }
          if (node->expression_class == ExpressionClass::BOUND_CONSTANT) {
            return true;
          }
          if (node->expression_class == ExpressionClass::BOUND_CAST) {
            auto &cast = node->Cast<BoundCastExpression>();
            return !cast.try_cast && cast.child &&
                   cast.child->expression_class ==
                       ExpressionClass::BOUND_CONSTANT;
          }
          return false;
        };

        auto is_column = [](const unique_ptr<Expression> &node) -> bool {
          if (!node) {
            return false;
          }
          return node->expression_class == ExpressionClass::BOUND_COLUMN_REF ||
                 node->expression_class == ExpressionClass::BOUND_REF;
        };

        if ((is_column(cmp.left) && is_constant(cmp.right)) ||
            (is_column(cmp.right) && is_constant(cmp.left))) {
          continue;
        }
      }
    }
    string filter_ir;
    if (!TryBuildLanceExprFilterIR(get, scan_bind.names, scan_bind.types, true,
                                   *expr, filter_ir)) {
#ifdef LANCE_VANE_DISTRIBUTED
      scan_bind.complex_filter_pushdown_failed = true;
#endif
      continue;
    }
    scan_bind.lance_pushed_filter_ir_parts.push_back(std::move(filter_ir));
  }
}

static bool LancePushdownExpression(ClientContext &, const LogicalGet &,
                                    Expression &expr) {
  if (expr.expression_class != ExpressionClass::BOUND_COMPARISON) {
    return false;
  }
  auto &cmp = expr.Cast<BoundComparisonExpression>();
  return cmp.type == ExpressionType::COMPARE_DISTINCT_FROM ||
         cmp.type == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

static unique_ptr<FunctionData>
LanceSearchVectorBind(ClientContext &context, TableFunctionBindInput &input,
                      vector<LogicalType> &return_types,
                      vector<string> &names) {
  if (input.inputs.size() < 3) {
    throw InvalidInputException(
        "lance_vector_search requires (path, vector_column, vector)");
  }
  if (input.inputs[0].IsNull()) {
    throw InvalidInputException(
        "lance_vector_search requires a dataset root path");
  }
  if (input.inputs[1].IsNull()) {
    throw InvalidInputException(
        "lance_vector_search requires a non-null vector_column");
  }
  if (input.inputs[2].IsNull()) {
    throw InvalidInputException(
        "lance_vector_search requires a non-null query vector");
  }

  auto result = make_uniq<LanceKnnBindData>();
#ifdef LANCE_VANE_DISTRIBUTED
  result->private_uri_diagnostics = LanceVanePathRequiresRedaction(
      context, input.inputs[0].GetValue<string>());
  result->vane_overload = VaneVectorOverload(input.inputs[2].type(), false);
#endif
  result->vector_column = input.inputs[1].GetValue<string>();
  result->query = ParseQueryVector(input.inputs[2], "lance_vector_search");
  result->prefilter = false;
  result->namespace_filter = ParseOptionalNamedString(input, "filter");

  auto verbose_it = input.named_parameters.find("explain_verbose");
  if (verbose_it != input.named_parameters.end() &&
      !verbose_it->second.IsNull()) {
    result->explain_verbose =
        verbose_it->second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
  }

  int64_t k_val = 10;
  auto k_named = input.named_parameters.find("k");
  if (k_named != input.named_parameters.end() && !k_named->second.IsNull()) {
    k_val =
        k_named->second.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
  }
  if (k_val <= 0) {
    throw InvalidInputException("lance_vector_search requires k > 0");
  }
  result->k = NumericCast<uint64_t>(k_val);

  bool has_nprobes = false;
  int64_t nprobes_val = 0;
  auto nprobes_named = input.named_parameters.find("nprobs");
  if (nprobes_named != input.named_parameters.end() &&
      !nprobes_named->second.IsNull()) {
    has_nprobes = true;
    nprobes_val = nprobes_named->second.DefaultCastAs(LogicalType::BIGINT)
                      .GetValue<int64_t>();
  }
  if (has_nprobes && nprobes_val <= 0) {
    throw InvalidInputException("lance_vector_search requires nprobs > 0");
  }
  result->nprobes = has_nprobes ? NumericCast<uint64_t>(nprobes_val) : 0;

  bool has_refine_factor = false;
  int64_t refine_factor_val = 0;
  auto refine_factor_named = input.named_parameters.find("refine_factor");
  if (refine_factor_named != input.named_parameters.end() &&
      !refine_factor_named->second.IsNull()) {
    has_refine_factor = true;
    refine_factor_val =
        refine_factor_named->second.DefaultCastAs(LogicalType::BIGINT)
            .GetValue<int64_t>();
  }
  if (has_refine_factor && refine_factor_val <= 0) {
    throw InvalidInputException(
        "lance_vector_search requires refine_factor > 0");
  }
  result->refine_factor =
      has_refine_factor ? NumericCast<uint64_t>(refine_factor_val) : 0;

  auto prefilter_named = input.named_parameters.find("prefilter");
  if (prefilter_named != input.named_parameters.end() &&
      !prefilter_named->second.IsNull()) {
    result->prefilter =
        prefilter_named->second.DefaultCastAs(LogicalType::BOOLEAN)
            .GetValue<bool>();
  }
  auto use_index_named = input.named_parameters.find("use_index");
  if (use_index_named != input.named_parameters.end() &&
      !use_index_named->second.IsNull()) {
    result->use_index =
        use_index_named->second.DefaultCastAs(LogicalType::BOOLEAN)
            .GetValue<bool>();
  }

  if (auto *table =
          TryResolveNamespaceBackedSearchTable(context, input.inputs[0])) {
    result->namespace_backed = true;
    result->namespace_config = table->NamespaceConfig();
    result->file_path = table->DatasetUri();
#ifdef LANCE_VANE_DISTRIBUTED
    result->dataset_entry =
        LanceGetOrOpenDatasetEntryForTable(context, *table, result->file_path);
    result->dataset =
        result->dataset_entry ? result->dataset_entry->Handle() : nullptr;
    if (!result->dataset) {
      throw IOException(
          "Failed to open an exact Lance namespace search snapshot: " +
          LanceVaneTableDiagnosticPath(*table, result->file_path) +
          LanceVaneTableFormatErrorSuffix(*table, result->file_path));
    }
    auto snapshot_version = lance_dataset_version(result->dataset);
    if (snapshot_version == 0 ||
        snapshot_version > NumericLimits<int64_t>::Maximum()) {
      throw IOException(
          "Failed to resolve an exact Lance namespace search version" +
          LanceVaneTableFormatErrorSuffix(*table, result->file_path));
    }
    result->namespace_config.snapshot_version = snapshot_version;
    result->private_uri_diagnostics =
        LanceVaneTablePathRequiresRedaction(*table, result->file_path);
#endif
    result->vector_column = RequireNamespaceSearchColumn(
        *table, result->vector_column, "lance_vector_search", "vector_column");
    if (result->prefilter && result->namespace_filter.empty()) {
      throw InvalidInputException(
          "lance_vector_search requires explicit filter when prefilter=true "
          "on namespace-backed tables");
    }
    PopulateNamespaceSearchSchema(
        context, *table, "_distance", result->schema_root, result->arrow_table,
        result->names, result->types, names, return_types);
#ifdef LANCE_VANE_DISTRIBUTED
    PrepareVaneSearchCandidate(
        context, input.inputs[0], result->dataset_entry, result->file_path,
        result->private_uri_diagnostics, result->vane_candidate);
    LancePrepareKnnVaneState(*result);
#endif
    return std::move(result);
  }

  if (!result->namespace_filter.empty()) {
    throw InvalidInputException(
        "lance_vector_search filter parameter is only supported for "
        "namespace-backed tables");
  }

  result->file_path.clear();
  result->dataset_entry =
      OpenSearchDatasetEntry(context, input.inputs[0], "lance_vector_search",
                             result->file_path, &result->dataset_cache_hit);
#ifdef LANCE_VANE_DISTRIBUTED
  if (auto *table = TryResolveLanceTableEntry(
          context, input.inputs[0].GetValue<string>())) {
    result->private_uri_diagnostics =
        LanceVaneTablePathRequiresRedaction(*table, result->file_path);
  } else {
    result->private_uri_diagnostics =
        result->private_uri_diagnostics ||
        LanceVanePathRequiresRedaction(context, result->file_path);
  }
#endif
  result->dataset =
      result->dataset_entry ? result->dataset_entry->Handle() : nullptr;

  if (!result->dataset) {
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException("Failed to open Lance dataset: " +
                      LanceVaneDiagnosticPath(result->file_path,
                                              result->private_uri_diagnostics) +
                      LanceVaneFormatErrorSuffix(
                          result->file_path, result->private_uri_diagnostics));
#else
    throw IOException("Failed to open Lance dataset: " + result->file_path +
                      LanceFormatErrorSuffix());
#endif
  }

  auto *schema_handle = lance_get_knn_schema(
      result->dataset, result->vector_column.c_str(), result->query.data(),
      result->query.size(), result->k, result->nprobes, result->refine_factor,
      result->prefilter ? 1 : 0, result->use_index ? 1 : 0);
  if (!schema_handle) {
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException("Failed to get Lance KNN schema: " +
                      LanceVaneDiagnosticPath(result->file_path,
                                              result->private_uri_diagnostics) +
                      LanceVaneFormatErrorSuffix(
                          result->file_path, result->private_uri_diagnostics));
#else
    throw IOException("Failed to get Lance KNN schema: " + result->file_path +
                      LanceFormatErrorSuffix());
#endif
  }

  memset(&result->schema_root.arrow_schema, 0,
         sizeof(result->schema_root.arrow_schema));
  if (lance_schema_to_arrow(schema_handle, &result->schema_root.arrow_schema) !=
      0) {
    lance_free_schema(schema_handle);
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException(
        "Failed to export Lance KNN schema to Arrow C Data Interface" +
        LanceVaneFormatErrorSuffix(result->file_path,
                                   result->private_uri_diagnostics));
#else
    throw IOException(
        "Failed to export Lance KNN schema to Arrow C Data Interface" +
        LanceFormatErrorSuffix());
#endif
  }
  lance_free_schema(schema_handle);
  LanceCoerceArrowSchemaForDuckDB(&result->schema_root.arrow_schema);
  ArrowTableFunction::PopulateArrowTableSchema(
      context, result->arrow_table, result->schema_root.arrow_schema);
  result->names = result->arrow_table.GetNames();
  result->types = result->arrow_table.GetTypes();
#ifdef LANCE_VANE_DISTRIBUTED
  PrepareVaneSearchCandidate(context, input.inputs[0], result->dataset_entry,
                             result->file_path, result->private_uri_diagnostics,
                             result->vane_candidate);
  LancePrepareKnnVaneState(*result);
#endif
  names = result->names;
  return_types = result->types;
  return std::move(result);
}

static unique_ptr<GlobalTableFunctionState>
LanceKnnInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<LanceKnnBindData>();
  auto state = make_uniq_base<GlobalTableFunctionState, LanceKnnGlobalState>();
  auto &global = state->Cast<LanceKnnGlobalState>();

  global.projection_ids = input.projection_ids;
  if (!input.projection_ids.empty()) {
    global.scanned_types.reserve(input.column_ids.size());
    for (auto col_id : input.column_ids) {
      if (col_id >= bind_data.types.size()) {
        throw IOException("Invalid column id in projection");
      }
      global.scanned_types.push_back(bind_data.types[col_id]);
    }
  }

#ifdef LANCE_VANE_DISTRIBUTED
  if (bind_data.vane_state.worker_bind) {
    LanceVaneValidateExecutionInput(input, bind_data.vane_state);
    global.vane_dataset_entry =
        LanceVaneOpenSearchSnapshot(context, bind_data.vane_state);
    global.vane_dataset = global.vane_dataset_entry->Handle();
    global.lance_filter_ir = bind_data.vane_state.final_filter_ir;
    global.filter_pushed_down = bind_data.vane_state.filter_pushed_down;
    return state;
  }
  if (bind_data.vane_state.execution_variant ==
      LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
    global.vane_dataset_entry = LanceVaneOpenSearchSnapshotForMaterialization(
        context, bind_data.vane_state);
    global.vane_dataset = global.vane_dataset_entry->Handle();
    global.lance_filter_ir = bind_data.vane_state.final_filter_ir;
    global.filter_pushed_down = bind_data.vane_state.filter_pushed_down;
    return state;
  }
#endif

  if (bind_data.namespace_backed) {
    return state;
  }

  auto table_filters = BuildLanceTableFilterIRParts(
      bind_data.names, bind_data.types, input, true);
  if (bind_data.prefilter && !table_filters.all_prefilterable_filters_pushed) {
    throw InvalidInputException("lance_vector_search requires filter pushdown "
                                "for prefilterable columns when "
                                "prefilter=true");
  }

  bool has_table_filter_parts = !table_filters.parts.empty();
  auto filter_parts = std::move(table_filters.parts);
  if (!bind_data.lance_pushed_filter_ir_parts.empty()) {
    filter_parts.reserve(filter_parts.size() +
                         bind_data.lance_pushed_filter_ir_parts.size());
    for (auto &part : bind_data.lance_pushed_filter_ir_parts) {
      filter_parts.push_back(part);
    }
  }

  string filter_ir_msg;
  if (!filter_parts.empty()) {
    if (!TryEncodeLanceFilterIRMessage(filter_parts, filter_ir_msg)) {
      filter_ir_msg.clear();
    }
    global.lance_filter_ir = std::move(filter_ir_msg);
  }
  if (bind_data.prefilter && has_table_filter_parts &&
      global.lance_filter_ir.empty()) {
    throw IOException("Failed to encode Lance filter IR");
  }
  global.filter_pushed_down =
      table_filters.all_filters_pushed && !global.lance_filter_ir.empty();
  return state;
}

static unique_ptr<LocalTableFunctionState>
LanceKnnLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                  GlobalTableFunctionState *global_state) {
  auto &bind_data = input.bind_data->Cast<LanceKnnBindData>();
  auto &global = global_state->Cast<LanceKnnGlobalState>();

  auto chunk = make_uniq<ArrowArrayWrapper>();
  auto result = make_uniq<LanceKnnLocalState>(std::move(chunk), context.client);
  result->column_ids = input.column_ids;
  result->filters = input.filters.get();
  result->global_state = &global;
  result->filter_pushed_down = global.filter_pushed_down;
  if (global.CanRemoveFilterColumns()) {
    result->all_columns.Initialize(context.client, global.scanned_types);
  }

#ifdef LANCE_VANE_DISTRIBUTED
  if (bind_data.vane_state.worker_bind ||
      bind_data.vane_state.execution_variant ==
          LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
    auto &search = bind_data.vane_state;
    const uint8_t *filter_ir =
        search.final_filter_ir.empty()
            ? nullptr
            : reinterpret_cast<const uint8_t *>(search.final_filter_ir.data());
    auto create_final_stream = [&](const uint8_t *ir, size_t ir_len) {
      return lance_vane_create_knn_stream_ir(
          global.vane_dataset, search.dataset_generation_id.c_str(),
          search.arguments.vector_column.c_str(),
          search.arguments.vector_query.data(),
          search.arguments.vector_query.size(), search.arguments.k,
          search.arguments.nprobes, search.arguments.refine_factor, ir, ir_len,
          search.namespace_filter_plan.empty()
              ? nullptr
              : reinterpret_cast<const uint8_t *>(
                    search.namespace_filter_plan.data()),
          search.namespace_filter_plan.size(),
          search.arguments.prefilter ? 1 : 0,
          search.arguments.use_index ? 1 : 0,
          reinterpret_cast<const uint8_t *>(search.index_plan.data()),
          search.index_plan.size());
    };
    if (search.execution_variant ==
        LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
      auto &fragment_ids = search.worker_bind ? search.selected_fragment_ids
                                              : search.fragment_ids;
      result->stream = lance_vane_create_vector_candidate_stream_ir(
          global.vane_dataset, search.dataset_generation_id.c_str(),
          search.arguments.vector_column.c_str(),
          search.arguments.vector_query.data(),
          search.arguments.vector_query.size(), search.arguments.k, filter_ir,
          search.final_filter_ir.size(),
          search.namespace_filter_plan.empty()
              ? nullptr
              : reinterpret_cast<const uint8_t *>(
                    search.namespace_filter_plan.data()),
          search.namespace_filter_plan.size(),
          search.arguments.prefilter ? 1 : 0,
          reinterpret_cast<const uint8_t *>(search.index_plan.data()),
          search.index_plan.size(), fragment_ids.data(), fragment_ids.size());
    } else {
      result->stream =
          create_final_stream(filter_ir, search.final_filter_ir.size());
      if (!result->stream && filter_ir && !search.arguments.prefilter) {
        global.filter_pushdown_fallbacks.fetch_add(1);
        global.filter_pushed_down = false;
        result->filter_pushed_down = false;
        result->stream = create_final_stream(nullptr, 0);
      }
    }
    if (!result->stream) {
      throw IOException(
          "Failed to create exact distributed Lance vector search stream" +
          LanceVaneFormatErrorSuffix(search.physical_uri,
                                     search.private_uri_diagnostics));
    }
    return result;
  }
#endif

  if (bind_data.namespace_backed) {
    vector<const char *> option_key_ptrs;
    vector<const char *> option_value_ptrs;
    vector<const char *> column_ptrs;
    string bearer_token;
    string api_key;
    LanceNamespaceQueryConfig config;
    FillLanceNamespaceQueryConfig(
        context.client, bind_data.namespace_config, bind_data.k,
        bind_data.prefilter, bind_data.namespace_filter,
        global.namespace_columns, option_key_ptrs, option_value_ptrs,
        column_ptrs, bearer_token, api_key, config);
    LanceNamespaceVectorSearchOptions options;
    options.vector_column = bind_data.vector_column.c_str();
    options.query_values = bind_data.query.data();
    options.query_len = bind_data.query.size();
    options.nprobes = bind_data.nprobes;
    options.refine_factor = bind_data.refine_factor;
    options.use_index = bind_data.use_index ? 1 : 0;
    result->stream =
        lance_create_namespace_vector_search_stream(&config, &options);
    if (!result->stream) {
#ifdef LANCE_VANE_DISTRIBUTED
      throw IOException(
          "Failed to create Lance namespace vector search stream" +
          LanceVaneFormatErrorSuffix(bind_data.file_path,
                                     bind_data.private_uri_diagnostics));
#else
      throw IOException("Failed to create Lance namespace vector search "
                        "stream" +
                        LanceFormatErrorSuffix());
#endif
    }
    return std::move(result);
  }

  const uint8_t *filter_ir =
      global.lance_filter_ir.empty()
          ? nullptr
          : reinterpret_cast<const uint8_t *>(global.lance_filter_ir.data());
  auto filter_ir_len = global.lance_filter_ir.size();
  result->stream = lance_create_knn_stream_ir(
      bind_data.dataset, bind_data.vector_column.c_str(),
      bind_data.query.data(), bind_data.query.size(), bind_data.k,
      bind_data.nprobes, bind_data.refine_factor, filter_ir, filter_ir_len,
      bind_data.prefilter ? 1 : 0, bind_data.use_index ? 1 : 0);
  if (!result->stream && filter_ir && !bind_data.prefilter) {
    // Best-effort: if filter pushdown failed, retry without it and rely on
    // DuckDB-side filter execution for correctness.
    global.filter_pushdown_fallbacks.fetch_add(1);
    global.filter_pushed_down = false;
    result->filter_pushed_down = false;
    result->stream = lance_create_knn_stream_ir(
        bind_data.dataset, bind_data.vector_column.c_str(),
        bind_data.query.data(), bind_data.query.size(), bind_data.k,
        bind_data.nprobes, bind_data.refine_factor, nullptr, 0,
        bind_data.prefilter ? 1 : 0, bind_data.use_index ? 1 : 0);
  }
  if (!result->stream) {
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException(
        "Failed to create Lance KNN stream" +
        LanceVaneFormatErrorSuffix(bind_data.file_path,
                                   bind_data.private_uri_diagnostics));
#else
    throw IOException("Failed to create Lance KNN stream" +
                      LanceFormatErrorSuffix());
#endif
  }

  return std::move(result);
}

#ifdef LANCE_VANE_DISTRIBUTED
static bool LanceKnnLoadNextBatch(LanceKnnLocalState &local_state,
                                  const LanceKnnBindData &bind_data) {
#else
static bool LanceKnnLoadNextBatch(LanceKnnLocalState &local_state) {
#endif
  if (!local_state.stream) {
    return false;
  }

  void *batch = nullptr;
  auto rc = lance_stream_next(local_state.stream, &batch);
  if (rc == 1) {
    lance_close_stream(local_state.stream);
    local_state.stream = nullptr;
    return false;
  }
  if (rc != 0) {
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException(
        "Failed to read next Lance RecordBatch" +
        LanceVaneFormatErrorSuffix(bind_data.file_path,
                                   bind_data.private_uri_diagnostics));
#else
    throw IOException("Failed to read next Lance RecordBatch" +
                      LanceFormatErrorSuffix());
#endif
  }

  auto new_chunk = make_shared_ptr<ArrowArrayWrapper>();
  memset(&new_chunk->arrow_array, 0, sizeof(new_chunk->arrow_array));
  ArrowSchema tmp_schema;
  memset(&tmp_schema, 0, sizeof(tmp_schema));

  if (lance_batch_to_arrow(batch, &new_chunk->arrow_array, &tmp_schema) != 0) {
    lance_free_batch(batch);
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException(
        "Failed to export Lance RecordBatch to Arrow C Data Interface" +
        LanceVaneFormatErrorSuffix(bind_data.file_path,
                                   bind_data.private_uri_diagnostics));
#else
    throw IOException(
        "Failed to export Lance RecordBatch to Arrow C Data Interface" +
        LanceFormatErrorSuffix());
#endif
  }

  lance_free_batch(batch);

  // Widen Float16 columns before DuckDB consumes the batch.
  LanceCoerceArrowArrayForDuckDB(&tmp_schema, &new_chunk->arrow_array);

  if (local_state.global_state) {
    local_state.global_state->record_batches.fetch_add(1);
    auto rows = NumericCast<idx_t>(new_chunk->arrow_array.length);
    local_state.global_state->record_batch_rows.fetch_add(rows);
  }

  if (tmp_schema.release) {
    tmp_schema.release(&tmp_schema);
  }

  local_state.chunk = std::move(new_chunk);
  local_state.Reset();
  return true;
}

static void LanceKnnFunc(ClientContext &context, TableFunctionInput &data,
                         DataChunk &output) {
  if (!data.local_state) {
    return;
  }

  auto &bind_data = data.bind_data->Cast<LanceKnnBindData>();
  auto &global_state = data.global_state->Cast<LanceKnnGlobalState>();
  auto &local_state = data.local_state->Cast<LanceKnnLocalState>();

  while (true) {
    if (local_state.chunk_offset >=
        NumericCast<idx_t>(local_state.chunk->arrow_array.length)) {
#ifdef LANCE_VANE_DISTRIBUTED
      if (!LanceKnnLoadNextBatch(local_state, bind_data)) {
#else
      if (!LanceKnnLoadNextBatch(local_state)) {
#endif
        return;
      }
    }

    auto remaining = NumericCast<idx_t>(local_state.chunk->arrow_array.length) -
                     local_state.chunk_offset;
    auto output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
    global_state.lines_read.fetch_add(output_size);

    if (global_state.CanRemoveFilterColumns()) {
      local_state.all_columns.Reset();
      local_state.all_columns.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(local_state,
                                        bind_data.arrow_table.GetColumns(),
                                        local_state.all_columns, false);
      local_state.chunk_offset += output_size;
      if (local_state.filters && !local_state.filter_pushed_down) {
        ApplyDuckDBFilters(context, *local_state.filters,
                           local_state.all_columns, local_state.filter_sel);
      }
      output.ReferenceColumns(local_state.all_columns,
                              global_state.projection_ids);
      output.SetCardinality(local_state.all_columns);
    } else {
      output.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(
          local_state, bind_data.arrow_table.GetColumns(), output, false);
      local_state.chunk_offset += output_size;
      if (local_state.filters && !local_state.filter_pushed_down) {
        ApplyDuckDBFilters(context, *local_state.filters, output,
                           local_state.filter_sel);
      }
    }

    if (output.size() == 0) {
      continue;
    }
    output.Verify();
    return;
  }
}

static InsertionOrderPreservingMap<string>
LanceKnnToString(TableFunctionToStringInput &input) {
  InsertionOrderPreservingMap<string> result;
  auto &bind_data = input.bind_data->Cast<LanceKnnBindData>();

#ifdef LANCE_VANE_DISTRIBUTED
  result["Lance Path"] = LanceVaneDiagnosticPath(
      bind_data.file_path, bind_data.private_uri_diagnostics);
#else
  result["Lance Path"] = bind_data.file_path;
#endif
  result["Lance Search Backend"] =
      bind_data.namespace_backed ? "namespace_query_table" : "dataset_scan";
  result["Lance Vector Column"] = bind_data.vector_column;
  result["Lance K"] = to_string(bind_data.k);
  result["Lance Nprobes"] = to_string(bind_data.nprobes);
  result["Lance Refine Factor"] = to_string(bind_data.refine_factor);
  result["Lance Query Dim"] = to_string(bind_data.query.size());
  result["Lance Prefilter"] = bind_data.prefilter ? "true" : "false";
  result["Lance Use Index"] = bind_data.use_index ? "true" : "false";
  result["Lance Explain Verbose"] =
      bind_data.explain_verbose ? "true" : "false";
  result["Lance Dataset Cache Hit"] =
      bind_data.dataset_cache_hit ? "true" : "false";
  if (!bind_data.namespace_filter.empty()) {
    result["Lance Namespace Filter"] = bind_data.namespace_filter;
  }

  if (bind_data.namespace_backed) {
    return result;
  }
#ifdef LANCE_VANE_DISTRIBUTED
  if (bind_data.private_uri_diagnostics) {
    return result;
  }
#endif

  result["Lance Pushed Filter Parts"] =
      to_string(bind_data.lance_pushed_filter_ir_parts.size());
  string filter_ir_msg;
  if (!bind_data.lance_pushed_filter_ir_parts.empty()) {
    TryEncodeLanceFilterIRMessage(bind_data.lance_pushed_filter_ir_parts,
                                  filter_ir_msg);
  }
  result["Lance Filter IR Bytes (Bind)"] = to_string(filter_ir_msg.size());

  string plan;
  string error;
  if (TryLanceExplainKnn(
          bind_data.dataset, bind_data.vector_column, bind_data.query,
          bind_data.k, bind_data.nprobes, bind_data.refine_factor,
          filter_ir_msg.empty() ? nullptr : &filter_ir_msg, bind_data.prefilter,
          bind_data.use_index, bind_data.explain_verbose, plan, error)) {
    result["Lance Plan (Bind)"] = plan;
  } else if (!error.empty()) {
    result["Lance Plan Error (Bind)"] = error;
  }

  return result;
}

static InsertionOrderPreservingMap<string>
LanceKnnDynamicToString(TableFunctionDynamicToStringInput &input) {
  InsertionOrderPreservingMap<string> result;
  auto &bind_data = input.bind_data->Cast<LanceKnnBindData>();
  auto &global_state = input.global_state->Cast<LanceKnnGlobalState>();

#ifdef LANCE_VANE_DISTRIBUTED
  result["Lance Path"] = LanceVaneDiagnosticPath(
      bind_data.file_path, bind_data.private_uri_diagnostics);
#else
  result["Lance Path"] = bind_data.file_path;
#endif
  result["Lance Search Backend"] =
      bind_data.namespace_backed ? "namespace_query_table" : "dataset_scan";
  result["Lance Vector Column"] = bind_data.vector_column;
  result["Lance K"] = to_string(bind_data.k);
  result["Lance Nprobes"] = to_string(bind_data.nprobes);
  result["Lance Refine Factor"] = to_string(bind_data.refine_factor);
  result["Lance Query Dim"] = to_string(bind_data.query.size());
  result["Lance Prefilter"] = bind_data.prefilter ? "true" : "false";
  result["Lance Use Index"] = bind_data.use_index ? "true" : "false";
  result["Lance Explain Verbose"] =
      bind_data.explain_verbose ? "true" : "false";
  result["Lance Dataset Cache Hit"] =
      bind_data.dataset_cache_hit ? "true" : "false";
  if (!bind_data.namespace_filter.empty()) {
    result["Lance Namespace Filter"] = bind_data.namespace_filter;
  }

  result["Lance Filter Pushed Down"] =
      global_state.filter_pushed_down ? "true" : "false";
  result["Lance Filter Pushdown Fallbacks"] =
      to_string(global_state.filter_pushdown_fallbacks.load());
  result["Lance Filter IR Bytes"] =
      to_string(global_state.lance_filter_ir.size());

  result["Lance Record Batches"] =
      to_string(global_state.record_batches.load());
  result["Lance Record Batch Rows"] =
      to_string(global_state.record_batch_rows.load());
  result["Lance Rows Out"] = to_string(global_state.lines_read.load());

  if (bind_data.namespace_backed) {
    return result;
  }
#ifdef LANCE_VANE_DISTRIBUTED
  if (bind_data.private_uri_diagnostics) {
    return result;
  }
#endif

  if (!global_state.explain_computed.load()) {
    std::lock_guard<std::mutex> guard(global_state.explain_mutex);
    if (!global_state.explain_computed.load()) {
      string plan;
      string error;
      auto ok = TryLanceExplainKnn(
          bind_data.dataset, bind_data.vector_column, bind_data.query,
          bind_data.k, bind_data.nprobes, bind_data.refine_factor,
          global_state.lance_filter_ir.empty() ? nullptr
                                               : &global_state.lance_filter_ir,
          bind_data.prefilter, bind_data.use_index, bind_data.explain_verbose,
          plan, error);
      if (ok) {
        global_state.explain_plan = std::move(plan);
      } else {
        global_state.explain_error = std::move(error);
      }
      global_state.explain_computed.store(true);
    }
  }

  if (!global_state.explain_plan.empty()) {
    result["Lance Plan"] = global_state.explain_plan;
  } else if (!global_state.explain_error.empty()) {
    result["Lance Plan Error"] = global_state.explain_error;
  }

  return result;
}

#ifdef LANCE_VANE_DISTRIBUTED
static LanceVaneSearchArguments
LanceKnnVaneArguments(const LanceKnnBindData &bind_data) {
  LanceVaneSearchArguments result;
  result.kind = LanceVaneSearchKind::VECTOR;
  result.overload = bind_data.vane_overload;
  result.vector_column = bind_data.vector_column;
  result.vector_query = bind_data.query;
  result.k = bind_data.k;
  result.nprobes = bind_data.nprobes;
  result.refine_factor = bind_data.refine_factor;
  result.prefilter = bind_data.prefilter;
  result.use_index = bind_data.use_index;
  result.explain_verbose = bind_data.explain_verbose;
  result.namespace_backed = bind_data.namespace_backed;
  result.namespace_filter = bind_data.namespace_filter;
  return result;
}

static void LancePrepareKnnVaneState(LanceKnnBindData &bind_data) {
  bind_data.vane_state = LanceVanePrepareGlobalSearchState(
      bind_data.vane_candidate, LanceKnnVaneArguments(bind_data),
      bind_data.names, bind_data.types);
}

static LanceVaneGlobalSearchState
LanceBuildKnnVaneState(const TableFunctionDistributedScanInput &input,
                       const LanceKnnBindData &bind_data) {
  if (bind_data.vane_state.worker_bind) {
    LanceVaneValidateDistributedInput(input, bind_data.vane_state);
    return bind_data.vane_state;
  }
  return LanceVaneFinalizeGlobalSearchState(
      input, bind_data.vane_state, bind_data.lance_pushed_filter_ir_parts,
      bind_data.complex_filter_pushdown_failed);
}

static vector<DistributedScanSplit> LancePlanDistributedKnnSearch(
    const TableFunctionDistributedScanPlanningInput &input) {
  auto &bind_data = input.bind_data->Cast<LanceKnnBindData>();
  auto state = LanceBuildKnnVaneState(input, bind_data);
  return LanceVaneCreateSearchTaskAssignments(state);
}

static unique_ptr<FunctionData> LanceCreateDistributedKnnWorkerBind(
    const TableFunctionDistributedScanInput &input) {
  auto &source = input.bind_data->Cast<LanceKnnBindData>();
  auto state = LanceBuildKnnVaneState(input, source);
  LanceVanePrepareSearchWorkerBindState(state);

  auto result = make_uniq<LanceKnnBindData>();
  result->column_ids = source.column_ids;
  result->file_path = state.physical_uri;
  result->private_uri_diagnostics = state.private_uri_diagnostics;
  result->vector_column = state.arguments.vector_column;
  result->query = state.arguments.vector_query;
  result->k = state.arguments.k;
  result->nprobes = state.arguments.nprobes;
  result->refine_factor = state.arguments.refine_factor;
  result->prefilter = state.arguments.prefilter;
  result->use_index = state.arguments.use_index;
  result->explain_verbose = state.arguments.explain_verbose;
  result->namespace_backed = false;
  result->namespace_filter = state.arguments.namespace_filter;
  if (state.execution_variant ==
      LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
    result->names = {"_rowid", "_distance"};
    result->types = {LogicalType::UBIGINT, LogicalType::FLOAT};
  } else {
    result->names = state.output_names;
    result->types = state.output_types;
  }
  result->vane_overload = state.arguments.overload;
  result->vane_state = std::move(state);
  return result;
}

static void
LanceApplyDistributedKnnSearch(optional_ptr<FunctionData> worker_bind,
                               const vector<DistributedScanSplit> &splits) {
  auto &bind_data = worker_bind->Cast<LanceKnnBindData>();
  LanceVaneApplySearchTaskAssignments(bind_data.vane_state, splits);
}

static void LanceKnnSerialize(Serializer &serializer,
                              const optional_ptr<FunctionData> bind_data,
                              const TableFunction &) {
  auto &data = bind_data->Cast<LanceKnnBindData>();
  auto state = data.vane_state;
  if (!state.finalized) {
    LanceVaneAccumulatePendingGlobalSearchFilters(
        state, data.lance_pushed_filter_ir_parts,
        data.complex_filter_pushdown_failed);
  }
  LanceVaneSerializeGlobalSearchState(serializer, state);
}

static unique_ptr<FunctionData> LanceKnnDeserialize(Deserializer &deserializer,
                                                    TableFunction &) {
  auto state = LanceVaneDeserializeGlobalSearchState(deserializer);
  if (state.arguments.kind != LanceVaneSearchKind::VECTOR ||
      (state.arguments.overload != LanceVaneSearchOverload::VECTOR_FLOAT &&
       state.arguments.overload != LanceVaneSearchOverload::VECTOR_DOUBLE)) {
    throw SerializationException(
        "Distributed Lance vector search overload identity mismatch");
  }
  auto result = make_uniq<LanceKnnBindData>();
  result->file_path = state.physical_uri;
  result->private_uri_diagnostics = state.private_uri_diagnostics;
  result->vector_column = state.arguments.vector_column;
  result->query = state.arguments.vector_query;
  result->k = state.arguments.k;
  result->nprobes = state.arguments.nprobes;
  result->refine_factor = state.arguments.refine_factor;
  result->prefilter = state.arguments.prefilter;
  result->use_index = state.arguments.use_index;
  result->explain_verbose = state.arguments.explain_verbose;
  result->namespace_filter = state.arguments.namespace_filter;
  if (state.execution_variant ==
      LanceVaneSearchTaskVariant::VECTOR_CANDIDATES) {
    result->names = {"_rowid", "_distance"};
    result->types = {LogicalType::UBIGINT, LogicalType::FLOAT};
  } else {
    result->names = state.output_names;
    result->types = state.output_types;
  }
  result->vane_overload = state.arguments.overload;
  result->vane_state = std::move(state);
  auto &context = deserializer.Get<ClientContext &>();
  LanceVanePopulateSearchSchema(context, result->names, result->types,
                                result->schema_root, result->arrow_table);
  return result;
}

static TableFunctionDistributedScanCallbacks
LanceKnnDistributedSearchCallbacks() {
  return LanceVaneSearchTaskCallbacks(LancePlanDistributedKnnSearch,
                                      LanceCreateDistributedKnnWorkerBind,
                                      LanceApplyDistributedKnnSearch);
}

struct LanceVectorMaterializeBindData : public TableFunctionData {
  LanceVaneGlobalSearchState vane_state;
  vector<string> names;
  vector<LogicalType> types;
  vector<string> take_columns;
  ArrowSchemaWrapper schema_root;
  ArrowTableSchema arrow_table;

  unique_ptr<FunctionData> Copy() const override {
    auto result = make_uniq<LanceVectorMaterializeBindData>();
    result->column_ids = column_ids;
    result->vane_state = vane_state;
    result->names = names;
    result->types = types;
    result->take_columns = take_columns;
    result->arrow_table = arrow_table;
    return result;
  }
};

struct LanceVectorMaterializeGlobalState : public GlobalTableFunctionState {
  shared_ptr<LanceDatasetCacheEntry> dataset_entry;
  void *dataset = nullptr;
  unordered_set<uint64_t> seen_row_ids;

  idx_t MaxThreads() const override { return 1; }
};

struct LanceVectorMaterializeLocalState : public ArrowScanLocalState {
  explicit LanceVectorMaterializeLocalState(
      unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &context)
      : ArrowScanLocalState(std::move(current_chunk), context) {}

  DataChunk materialized_columns;
  vector<ColumnIndex> output_columns;
  vector<column_t> output_arrow_column_ids;
};

static unique_ptr<FunctionData>
LanceVectorMaterializeBind(ClientContext &, TableFunctionBindInput &,
                           vector<LogicalType> &, vector<string> &) {
  throw BinderException(
      "__lance_vector_search_materialize is an internal table function");
}

static vector<ColumnIndex>
LanceVectorMaterializeColumns(const LanceVaneGlobalSearchState &state) {
  if (state.output_names.empty() ||
      state.output_names.size() != state.output_types.size() ||
      state.output_names.back() != "_distance") {
    throw SerializationException(
        "Distributed Lance vector materialization schema is malformed");
  }
  auto append_column = [&](vector<ColumnIndex> &result,
                           const ColumnIndex &column) {
    if (!column.HasPrimaryIndex() || column.IsVirtualColumn()) {
      throw SerializationException(
          "Distributed Lance vector materialization cannot reference a "
          "virtual or non-primary column");
    }
    auto column_id = column.GetPrimaryIndex();
    if (column_id >= state.output_names.size()) {
      throw SerializationException(
          "Distributed Lance vector materialization column is malformed");
    }
    result.push_back(column);
  };

  vector<ColumnIndex> result;
  if (state.projection_ids.empty()) {
    result.reserve(state.column_ids.size());
    for (auto &column : state.column_ids) {
      append_column(result, column);
    }
    return result;
  }
  result.reserve(state.projection_ids.size());
  for (auto projection_id : state.projection_ids) {
    if (projection_id >= state.column_ids.size()) {
      throw SerializationException(
          "Distributed Lance vector materialization projection is malformed");
    }
    append_column(result, state.column_ids[projection_id]);
  }
  return result;
}

struct LanceVectorMaterializeProjection {
  vector<string> take_columns;
  vector<string> arrow_names;
  vector<LogicalType> arrow_types;
  vector<ColumnIndex> output_columns;
  vector<column_t> arrow_column_ids;
};

static LanceVectorMaterializeProjection
LanceBuildVectorMaterializeProjection(const LanceVaneGlobalSearchState &state) {
  auto output_columns = LanceVectorMaterializeColumns(state);
  auto distance_column_id = state.output_names.size() - 1;
  LanceVectorMaterializeProjection result;
  unordered_map<column_t, column_t> arrow_positions;
  // Preserve every DuckDB child path for the output mapping. Only the
  // top-level physical columns sent to Lance take_rows are deduplicated.
  result.output_columns = output_columns;
  result.arrow_column_ids.reserve(output_columns.size());
  for (auto &column : output_columns) {
    auto column_id = column.GetPrimaryIndex();
    if (column_id == distance_column_id) {
      if (column.HasChildren()) {
        throw SerializationException(
            "Distributed Lance vector materialization distance column has "
            "an invalid child path");
      }
      continue;
    }
    if (column_id > distance_column_id) {
      throw SerializationException(
          "Distributed Lance vector materialization column is malformed");
    }
    if (arrow_positions.find(column_id) != arrow_positions.end()) {
      continue;
    }
    auto arrow_position = NumericCast<column_t>(result.arrow_names.size());
    arrow_positions.emplace(column_id, arrow_position);
    result.take_columns.push_back(state.output_names[column_id]);
    result.arrow_names.push_back(state.output_names[column_id]);
    result.arrow_types.push_back(state.output_types[column_id]);
  }
  auto distance_arrow_position =
      NumericCast<column_t>(result.arrow_names.size());
  result.arrow_names.push_back(state.output_names[distance_column_id]);
  result.arrow_types.push_back(state.output_types[distance_column_id]);
  for (auto &column : output_columns) {
    auto column_id = column.GetPrimaryIndex();
    if (column_id == distance_column_id) {
      result.arrow_column_ids.push_back(distance_arrow_position);
      continue;
    }
    auto position = arrow_positions.find(column_id);
    if (position == arrow_positions.end()) {
      throw InternalException(
          "Missing distributed Lance vector materialization column mapping");
    }
    result.arrow_column_ids.push_back(position->second);
  }
  return result;
}

static void LancePrepareVectorMaterializeBindData(
    ClientContext &context, LanceVectorMaterializeBindData &bind_data) {
  auto projection = LanceBuildVectorMaterializeProjection(bind_data.vane_state);
  bind_data.take_columns = std::move(projection.take_columns);
  bind_data.names = std::move(projection.arrow_names);
  bind_data.types = std::move(projection.arrow_types);
  LanceVanePopulateSearchSchema(context, bind_data.names, bind_data.types,
                                bind_data.schema_root, bind_data.arrow_table);
}

static vector<LogicalType>
LanceVectorMaterializeOutputTypes(const LanceVaneGlobalSearchState &state) {
  auto columns = LanceVectorMaterializeColumns(state);
  vector<LogicalType> result;
  result.reserve(columns.size());
  for (auto &column : columns) {
    auto column_id = column.GetPrimaryIndex();
    if (column_id >= state.output_types.size()) {
      throw SerializationException(
          "Distributed Lance vector materialization schema is malformed");
    }
    if (column.IsPushdownExtract()) {
      if (!column.HasType() || !column.HasChildren()) {
        throw SerializationException(
            "Distributed Lance vector materialization extract is malformed");
      }
      result.push_back(column.GetScanType());
    } else {
      result.push_back(state.output_types[column_id]);
    }
  }
  return result;
}

static Vector &LanceVectorMaterializeOutputVector(Vector &root,
                                                  const ColumnIndex &column) {
  if (!column.IsPushdownExtract()) {
    // Child indexes on a normal ColumnIndex are optional DuckDB pruning hints.
    // The projection above the scan still expects the complete root value.
    return root;
  }

  auto *current = &root;
  auto *path = &column;
  while (path->HasChildren()) {
    if (path->ChildIndexCount() != 1 ||
        current->GetType().id() != LogicalTypeId::STRUCT) {
      throw SerializationException(
          "Distributed Lance vector materialization extract path is "
          "malformed");
    }
    auto &child = path->GetChildIndex(0);
    if (!child.HasPrimaryIndex() || child.IsVirtualColumn()) {
      throw SerializationException(
          "Distributed Lance vector materialization extract path must use "
          "primary struct indexes");
    }
    auto child_id = child.GetPrimaryIndex();
    auto &entries = StructVector::GetEntries(*current);
    if (child_id >= entries.size()) {
      throw SerializationException(
          "Distributed Lance vector materialization extract path is out of "
          "range");
    }
    current = entries[child_id].get();
    path = &child;
  }
  if (!column.HasType() || current->GetType() != column.GetScanType()) {
    throw SerializationException(
        "Distributed Lance vector materialization extract type is malformed");
  }
  return *current;
}

static unique_ptr<GlobalTableFunctionState>
LanceVectorMaterializeInitGlobal(ClientContext &context,
                                 TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<LanceVectorMaterializeBindData>();
  if (input.column_indexes != bind_data.vane_state.column_ids) {
    throw InvalidInputException(
        "Distributed Lance vector materialization projection changed after "
        "admission");
  }
  auto result = make_uniq_base<GlobalTableFunctionState,
                               LanceVectorMaterializeGlobalState>();
  auto &global = result->Cast<LanceVectorMaterializeGlobalState>();
  global.dataset_entry = LanceVaneOpenSearchSnapshotForMaterialization(
      context, bind_data.vane_state);
  global.dataset = global.dataset_entry->Handle();
  return result;
}

static unique_ptr<LocalTableFunctionState>
LanceVectorMaterializeInitLocal(ExecutionContext &context,
                                TableFunctionInitInput &input,
                                GlobalTableFunctionState *) {
  auto &bind_data = input.bind_data->Cast<LanceVectorMaterializeBindData>();
  auto chunk = make_uniq<ArrowArrayWrapper>();
  auto result = make_uniq<LanceVectorMaterializeLocalState>(std::move(chunk),
                                                            context.client);
  auto projection = LanceBuildVectorMaterializeProjection(bind_data.vane_state);
  result->output_columns = std::move(projection.output_columns);
  result->output_arrow_column_ids = std::move(projection.arrow_column_ids);
  result->column_ids.reserve(projection.arrow_types.size());
  for (idx_t column_id = 0; column_id < projection.arrow_types.size();
       column_id++) {
    result->column_ids.push_back(column_id);
  }
  result->materialized_columns.Initialize(context.client,
                                          projection.arrow_types);
  return result;
}

static OperatorResultType LanceVectorMaterializeFunc(ExecutionContext &,
                                                     TableFunctionInput &data,
                                                     DataChunk &input,
                                                     DataChunk &output) {
  if (input.size() == 0) {
    return OperatorResultType::NEED_MORE_INPUT;
  }
  if (input.ColumnCount() != 2 ||
      input.data[0].GetType() != LogicalType::UBIGINT ||
      input.data[1].GetType() != LogicalType::FLOAT) {
    throw InvalidInputException(
        "Distributed Lance vector materialization received an invalid "
        "candidate schema");
  }

  auto &bind_data = data.bind_data->Cast<LanceVectorMaterializeBindData>();
  auto &global = data.global_state->Cast<LanceVectorMaterializeGlobalState>();
  auto &local = data.local_state->Cast<LanceVectorMaterializeLocalState>();
  UnifiedVectorFormat row_id_format;
  UnifiedVectorFormat distance_format;
  input.data[0].ToUnifiedFormat(input.size(), row_id_format);
  input.data[1].ToUnifiedFormat(input.size(), distance_format);
  auto row_id_data = UnifiedVectorFormat::GetData<uint64_t>(row_id_format);
  auto distance_data = UnifiedVectorFormat::GetData<float>(distance_format);
  vector<uint64_t> row_ids;
  vector<float> distances;
  row_ids.reserve(input.size());
  distances.reserve(input.size());
  for (idx_t row = 0; row < input.size(); row++) {
    auto row_id_index = row_id_format.sel->get_index(row);
    auto distance_index = distance_format.sel->get_index(row);
    if (!row_id_format.validity.RowIsValid(row_id_index) ||
        !distance_format.validity.RowIsValid(distance_index) ||
        !std::isfinite(distance_data[distance_index])) {
      throw InvalidInputException(
          "Distributed Lance vector materialization received an invalid "
          "candidate");
    }
    auto row_id = row_id_data[row_id_index];
    if (!global.seen_row_ids.insert(row_id).second) {
      throw InvalidInputException(
          "Distributed Lance vector materialization received duplicate row "
          "id %llu at candidate offset %llu after observing %llu unique "
          "rows",
          row_id, row, global.seen_row_ids.size());
    }
    row_ids.push_back(row_id);
    distances.push_back(distance_data[distance_index]);
  }

  vector<const char *> take_columns;
  take_columns.reserve(bind_data.take_columns.size());
  for (auto &column : bind_data.take_columns) {
    take_columns.push_back(column.c_str());
  }
  auto *batch = lance_vane_take_vector_rows(
      global.dataset, row_ids.data(), distances.data(), row_ids.size(),
      take_columns.data(), take_columns.size());
  if (!batch) {
    throw IOException(
        "Failed to materialize distributed Lance vector search rows" +
        LanceVaneFormatErrorSuffix(
            bind_data.vane_state.physical_uri,
            bind_data.vane_state.private_uri_diagnostics));
  }
  auto new_chunk = make_shared_ptr<ArrowArrayWrapper>();
  memset(&new_chunk->arrow_array, 0, sizeof(new_chunk->arrow_array));
  ArrowSchema batch_schema;
  memset(&batch_schema, 0, sizeof(batch_schema));
  if (lance_batch_to_arrow(batch, &new_chunk->arrow_array, &batch_schema) !=
      0) {
    lance_free_batch(batch);
    throw IOException(
        "Failed to export distributed Lance vector materialization batch" +
        LanceVaneFormatErrorSuffix(
            bind_data.vane_state.physical_uri,
            bind_data.vane_state.private_uri_diagnostics));
  }
  lance_free_batch(batch);
  LanceCoerceArrowArrayForDuckDB(&batch_schema, &new_chunk->arrow_array);
  if (batch_schema.release) {
    batch_schema.release(&batch_schema);
  }

  local.chunk = std::move(new_chunk);
  local.Reset();
  local.materialized_columns.Reset();
  local.materialized_columns.SetCardinality(input.size());
  ArrowTableFunction::ArrowToDuckDB(local, bind_data.arrow_table.GetColumns(),
                                    local.materialized_columns, false);
  local.chunk_offset += input.size();

  if (output.ColumnCount() != local.output_columns.size() ||
      output.ColumnCount() != local.output_arrow_column_ids.size()) {
    throw InternalException(
        "Distributed Lance vector materialization output mapping changed");
  }
  output.SetCardinality(input.size());
  for (idx_t output_id = 0; output_id < output.ColumnCount(); output_id++) {
    auto arrow_id = local.output_arrow_column_ids[output_id];
    if (arrow_id >= local.materialized_columns.ColumnCount()) {
      throw InternalException(
          "Distributed Lance vector materialization output is out of range");
    }
    auto &source = local.materialized_columns.data[arrow_id];
    auto &selected = LanceVectorMaterializeOutputVector(
        source, local.output_columns[output_id]);
    if (selected.GetType() != output.data[output_id].GetType()) {
      throw SerializationException(
          "Distributed Lance vector materialization output type changed");
    }
    output.data[output_id].Reference(selected);
  }
  output.Verify();
  return OperatorResultType::NEED_MORE_INPUT;
}

static OperatorResultType
LanceVectorMaterializeBatchFunc(ExecutionContext &context,
                                TableFunctionInput &data, ExecutionBatch &input,
                                ExecutionBatch &output) {
  if (input.kind != ExecutionBatchKind::MATERIALIZED_CHUNK) {
    throw InvalidInputException(
        "Distributed Lance vector materialization requires a native TopN "
        "batch");
  }
  if (!input.materialized) {
    if (input.rows != 0) {
      throw InvalidInputException(
          "Distributed Lance vector materialization received a missing "
          "candidate batch");
    }
    input.materialized = make_uniq<DataChunk>();
    input.materialized->Initialize(
        BufferAllocator::Get(context.client),
        vector<LogicalType>{LogicalType::UBIGINT, LogicalType::FLOAT});
  }
  auto output_chunk = make_uniq<DataChunk>();
  auto &bind_data = data.bind_data->Cast<LanceVectorMaterializeBindData>();
  output_chunk->Initialize(
      BufferAllocator::Get(context.client),
      LanceVectorMaterializeOutputTypes(bind_data.vane_state));
  auto result = LanceVectorMaterializeFunc(context, data, *input.materialized,
                                           *output_chunk);
  output = ExecutionBatch();
  output.kind = ExecutionBatchKind::MATERIALIZED_CHUNK;
  output.rows = output_chunk->size();
  output.estimated_bytes = output_chunk->GetAllocationSize();
  output.materialized = std::move(output_chunk);
  return result;
}

static void
LanceVectorMaterializeSerialize(Serializer &serializer,
                                const optional_ptr<FunctionData> bind_data,
                                const TableFunction &) {
  auto &data = bind_data->Cast<LanceVectorMaterializeBindData>();
  LanceVaneSerializeGlobalSearchState(serializer, data.vane_state);
}

static unique_ptr<FunctionData>
LanceVectorMaterializeDeserialize(Deserializer &deserializer, TableFunction &) {
  auto state = LanceVaneDeserializeGlobalSearchState(deserializer);
  if (state.execution_variant !=
          LanceVaneSearchTaskVariant::VECTOR_CANDIDATES ||
      state.arguments.kind != LanceVaneSearchKind::VECTOR ||
      state.worker_bind) {
    throw SerializationException(
        "Distributed Lance vector materialization state is malformed");
  }
  auto result = make_uniq<LanceVectorMaterializeBindData>();
  result->vane_state = std::move(state);
  auto &context = deserializer.Get<ClientContext &>();
  LancePrepareVectorMaterializeBindData(context, *result);
  return result;
}

static TableFunction LanceVectorMaterializeFunction() {
  TableFunction result(
      "__lance_vector_search_materialize", {LogicalType::TABLE}, nullptr,
      LanceVectorMaterializeBind, LanceVectorMaterializeInitGlobal,
      LanceVectorMaterializeInitLocal);
  result.in_out_function = LanceVectorMaterializeFunc;
  result.in_out_function_batch = LanceVectorMaterializeBatchFunc;
  result.serialize = LanceVectorMaterializeSerialize;
  result.deserialize = LanceVectorMaterializeDeserialize;
  result.projection_pushdown = true;
  return result;
}

static unique_ptr<LogicalOperator>
LanceRewriteExactVectorCandidates(ClientContext &context, Optimizer &optimizer,
                                  unique_ptr<LogicalOperator> op,
                                  vector<const Expression *> ancestor_filters) {
  if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
    auto &filter = op->Cast<LogicalFilter>();
    for (auto &expression : filter.expressions) {
      if (!expression) {
        return op;
      }
      ancestor_filters.push_back(expression.get());
    }
  }
  for (auto &child : op->children) {
    child = LanceRewriteExactVectorCandidates(
        context, optimizer, std::move(child), ancestor_filters);
  }
  if (op->type != LogicalOperatorType::LOGICAL_GET) {
    return op;
  }
  auto &get = op->Cast<LogicalGet>();
  if (get.function.name != "lance_vector_search" || !get.bind_data ||
      !get.children.empty()) {
    return op;
  }
  auto &bind_data = get.bind_data->Cast<LanceKnnBindData>();
  if (bind_data.vane_state.worker_bind ||
      bind_data.vane_state.execution_variant !=
          LanceVaneSearchTaskVariant::FINAL_SEARCH) {
    return op;
  }
  if (bind_data.complex_filter_pushdown_failed) {
    // This can represent a computed or mixed postfilter that the normal
    // FINAL_SEARCH path preserves above the scan. Candidate admission must
    // not finalize it as though every predicate were a Lance prefilter.
    return op;
  }
  for (auto &entry : get.table_filters.filters) {
    auto scan_index = NumericCast<idx_t>(entry.first);
    auto &column_ids = get.GetColumnIds();
    if (scan_index >= column_ids.size()) {
      return op;
    }
    auto &column_index = column_ids[scan_index];
    if (column_index.IsVirtualColumn() || !column_index.HasPrimaryIndex()) {
      return op;
    }
    auto column_id = column_index.GetPrimaryIndex();
    if (column_id >= bind_data.names.size() ||
        IsComputedSearchColumn(bind_data.names[column_id])) {
      // DuckDB can represent a computed score postfilter in TableFilterSet
      // while retaining a base-column safety filter above the scan.
      return op;
    }
  }
  bool has_base_filter_ancestor = false;
  for (auto *expression : ancestor_filters) {
    auto reference_class =
        ClassifyVaneSearchColumnReferences(get, bind_data.names, *expression);
    if (reference_class != LanceVaneSearchColumnReferenceClass::BASE_ONLY) {
      // Computed, constant, mixed, and unresolved residual predicates are
      // postfilters. Leave FINAL_SEARCH intact; its normal planning path also
      // retains the established strict error for an incomplete prefilter.
      return op;
    }
    has_base_filter_ancestor = true;
  }

  TableFunctionDistributedScanInput distributed_input(
      get.bind_data.get(), get.parameters, get.GetColumnIds(),
      get.projection_ids, &get.table_filters, get.estimated_cardinality);
  auto state = LanceVaneFinalizeGlobalSearchState(
      distributed_input, bind_data.vane_state,
      bind_data.lance_pushed_filter_ir_parts,
      bind_data.complex_filter_pushdown_failed);
  auto has_postfilter = !state.arguments.prefilter && has_base_filter_ancestor;
  if (!state.arguments.prefilter) {
    has_postfilter = has_postfilter || !get.table_filters.filters.empty() ||
                     !bind_data.lance_pushed_filter_ir_parts.empty() ||
                     bind_data.complex_filter_pushdown_failed ||
                     !bind_data.namespace_filter.empty();
  }
  if (!LanceVaneTryEnableExactVectorCandidates(state, has_postfilter)) {
    return op;
  }

  vector<string> candidate_names = {"_rowid", "_distance"};
  vector<LogicalType> candidate_types = {LogicalType::UBIGINT,
                                         LogicalType::FLOAT};
  auto candidate_bind = bind_data.Copy();
  auto &candidate_data = candidate_bind->Cast<LanceKnnBindData>();
  candidate_data.column_ids = {0, 1};
  candidate_data.names = candidate_names;
  candidate_data.types = candidate_types;
  candidate_data.arrow_table = ArrowTableSchema();
  candidate_data.lance_pushed_filter_ir_parts.clear();
  candidate_data.complex_filter_pushdown_failed = false;
  candidate_data.vane_state = state;
  LanceVanePopulateSearchSchema(context, candidate_names, candidate_types,
                                candidate_data.schema_root,
                                candidate_data.arrow_table);

  auto candidate_table_index = optimizer.binder.GenerateTableIndex();
  auto candidate_get = make_uniq<LogicalGet>(
      candidate_table_index, get.function, std::move(candidate_bind),
      candidate_types, candidate_names);
  candidate_get->parameters = get.parameters;
  candidate_get->named_parameters = get.named_parameters;
  candidate_get->SetColumnIds(
      vector<ColumnIndex>{ColumnIndex(0), ColumnIndex(1)});
  auto max_cardinality = NumericLimits<idx_t>::Maximum();
  auto k = NumericCast<idx_t>(state.arguments.k);
  auto fragment_count = state.fragment_ids.size();
  auto candidate_cardinality =
      fragment_count > 0 && k > max_cardinality / fragment_count
          ? max_cardinality
          : k * fragment_count;
  candidate_get->SetEstimatedCardinality(candidate_cardinality);

  vector<BoundOrderByNode> orders;
  orders.emplace_back(
      OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
      make_uniq<BoundColumnRefExpression>(
          LogicalType::FLOAT, ColumnBinding(candidate_table_index, 1)));
  orders.emplace_back(
      OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
      make_uniq<BoundColumnRefExpression>(
          LogicalType::UBIGINT, ColumnBinding(candidate_table_index, 0)));
  auto top_k = make_uniq<LogicalTopN>(std::move(orders), k, 0);
  top_k->children.push_back(std::move(candidate_get));
  top_k->SetEstimatedCardinality(k);

  auto materialize_bind = make_uniq<LanceVectorMaterializeBindData>();
  materialize_bind->vane_state = state;
  LancePrepareVectorMaterializeBindData(context, *materialize_bind);
  auto materialize_get = make_uniq<LogicalGet>(
      get.table_index, LanceVectorMaterializeFunction(),
      std::move(materialize_bind), state.output_types, state.output_names);
  materialize_get->SetColumnIds(vector<ColumnIndex>(get.GetColumnIds().begin(),
                                                    get.GetColumnIds().end()));
  materialize_get->projection_ids = get.projection_ids;
  materialize_get->input_table_types = candidate_types;
  materialize_get->input_table_names = candidate_names;
  materialize_get->children.push_back(std::move(top_k));
  materialize_get->SetEstimatedCardinality(k);
  return std::move(materialize_get);
}

static void
LanceExactVectorCandidatesOptimizer(OptimizerExtensionInput &input,
                                    unique_ptr<LogicalOperator> &plan) {
  plan = LanceRewriteExactVectorCandidates(input.context, input.optimizer,
                                           std::move(plan), {});
}
#endif

static void RegisterLanceVectorSearch(ExtensionLoader &loader) {
  auto configure = [](TableFunction &fun) {
    fun.named_parameters["k"] = LogicalType::BIGINT;
    fun.named_parameters["nprobs"] = LogicalType::BIGINT;
    fun.named_parameters["refine_factor"] = LogicalType::BIGINT;
    fun.named_parameters["prefilter"] = LogicalType::BOOLEAN;
    fun.named_parameters["use_index"] = LogicalType::BOOLEAN;
    fun.named_parameters["explain_verbose"] = LogicalType::BOOLEAN;
    fun.named_parameters["filter"] = LogicalType::VARCHAR;
    fun.projection_pushdown = true;
    fun.filter_pushdown = true;
    fun.filter_prune = true;
    fun.pushdown_expression = LancePushdownExpression;
    fun.pushdown_complex_filter = LancePushdownComplexFilter;
    fun.to_string = LanceKnnToString;
    fun.dynamic_to_string = LanceKnnDynamicToString;
#ifdef LANCE_VANE_DISTRIBUTED
    fun.serialize = LanceKnnSerialize;
    fun.deserialize = LanceKnnDeserialize;
    fun.SetDistributedScanCallbacks(LanceKnnDistributedSearchCallbacks());
#endif
  };

  TableFunction search_f32("lance_vector_search",
                           {LogicalType::VARCHAR, LogicalType::VARCHAR,
                            LogicalType::LIST(LogicalType::FLOAT)},
                           LanceKnnFunc, LanceSearchVectorBind,
                           LanceKnnInitGlobal, LanceKnnLocalInit);
  configure(search_f32);
  loader.RegisterFunction(search_f32);

  TableFunction search_f64("lance_vector_search",
                           {LogicalType::VARCHAR, LogicalType::VARCHAR,
                            LogicalType::LIST(LogicalType::DOUBLE)},
                           LanceKnnFunc, LanceSearchVectorBind,
                           LanceKnnInitGlobal, LanceKnnLocalInit);
  configure(search_f64);
  loader.RegisterFunction(search_f64);
#ifdef LANCE_VANE_DISTRIBUTED
  auto materialize = LanceVectorMaterializeFunction();
  loader.RegisterFunction(materialize);
#endif
}

// --- FTS / hybrid search ---

enum class LanceSearchMode : uint8_t { Fts = 0, Hybrid = 1 };

struct LanceSearchBindData : public TableFunctionData {
  LanceSearchMode mode = LanceSearchMode::Fts;

  string file_path;
#ifdef LANCE_VANE_DISTRIBUTED
  bool private_uri_diagnostics = false;
#endif
  bool prefilter = false;
  bool namespace_backed = false;
  LanceNamespaceTableConfig namespace_config;
  string namespace_filter;

  // FTS mode
  string text_column;
  string query;

  // Hybrid mode
  string vector_column;
  vector<float> vector_query;
  string text_query;
  uint64_t nprobes = 0;
  uint64_t refine_factor = 0;
  bool use_index = true;
  float alpha = 0.5F;
  uint32_t oversample_factor = 4;

  uint64_t k = 10;

  shared_ptr<LanceDatasetCacheEntry> dataset_entry;
  void *dataset = nullptr;
  bool dataset_cache_hit = false;
  ArrowSchemaWrapper schema_root;
  ArrowTableSchema arrow_table;
  vector<string> names;
  vector<LogicalType> types;
#ifdef LANCE_VANE_DISTRIBUTED
  vector<string> lance_pushed_filter_ir_parts;
  bool complex_filter_pushdown_failed = false;
  LanceVaneSearchOverload vane_overload = LanceVaneSearchOverload::FTS;
  LanceVanePhysicalCandidate vane_candidate;
  LanceVaneGlobalSearchState vane_state;

  unique_ptr<FunctionData> Copy() const override {
    auto result = make_uniq<LanceSearchBindData>();
    result->column_ids = column_ids;
    result->mode = mode;
    result->file_path = file_path;
    result->private_uri_diagnostics = private_uri_diagnostics;
    result->prefilter = prefilter;
    result->namespace_backed = namespace_backed;
    result->namespace_config = namespace_config;
    result->namespace_filter = namespace_filter;
    result->text_column = text_column;
    result->query = query;
    result->vector_column = vector_column;
    result->vector_query = vector_query;
    result->text_query = text_query;
    result->nprobes = nprobes;
    result->refine_factor = refine_factor;
    result->use_index = use_index;
    result->alpha = alpha;
    result->oversample_factor = oversample_factor;
    result->k = k;
    result->dataset_entry = dataset_entry;
    result->dataset = dataset;
    result->dataset_cache_hit = dataset_cache_hit;
    result->arrow_table = arrow_table;
    result->names = names;
    result->types = types;
    result->lance_pushed_filter_ir_parts = lance_pushed_filter_ir_parts;
    result->complex_filter_pushdown_failed = complex_filter_pushdown_failed;
    result->vane_overload = vane_overload;
    result->vane_candidate = vane_candidate;
    result->vane_state = vane_state;
    return result;
  }
#endif
};

#ifdef LANCE_VANE_DISTRIBUTED
static void LancePrepareSharedVaneState(LanceSearchBindData &bind_data);
#endif

struct LanceSearchGlobalState : public GlobalTableFunctionState {
  std::atomic<idx_t> lines_read{0};
  std::atomic<idx_t> record_batches{0};
  std::atomic<idx_t> record_batch_rows{0};
  string lance_filter_ir;
  bool filter_pushed_down = false;
  std::atomic<idx_t> filter_pushdown_fallbacks{0};

  vector<idx_t> projection_ids;
  vector<LogicalType> scanned_types;
  vector<string> namespace_columns;

#ifdef LANCE_VANE_DISTRIBUTED
  shared_ptr<LanceDatasetCacheEntry> vane_dataset_entry;
  void *vane_dataset = nullptr;
#endif

  idx_t MaxThreads() const override { return 1; }
  bool CanRemoveFilterColumns() const { return !projection_ids.empty(); }
};

#ifdef LANCE_VANE_DISTRIBUTED
static void
LanceSearchPushdownComplexFilter(ClientContext &, LogicalGet &get,
                                 FunctionData *bind_data,
                                 vector<unique_ptr<Expression>> &filters) {
  if (!bind_data || filters.empty()) {
    return;
  }
  auto &scan_bind = bind_data->Cast<LanceSearchBindData>();
  if (scan_bind.namespace_backed) {
    return;
  }
  for (auto &expr : filters) {
    if (expr &&
        ClassifyVaneSearchColumnReferences(get, scan_bind.names, *expr) ==
            LanceVaneSearchColumnReferenceClass::COMPUTED_ONLY) {
      continue;
    }
    if (!expr || expr->HasParameter() || expr->IsVolatile()) {
      scan_bind.complex_filter_pushdown_failed = true;
      continue;
    }
    string filter_ir;
    if (!TryBuildLanceExprFilterIR(get, scan_bind.names, scan_bind.types, true,
                                   *expr, filter_ir)) {
      scan_bind.complex_filter_pushdown_failed = true;
      continue;
    }
    scan_bind.lance_pushed_filter_ir_parts.push_back(std::move(filter_ir));
  }
}
#endif

struct LanceSearchLocalState : public ArrowScanLocalState {
  explicit LanceSearchLocalState(unique_ptr<ArrowArrayWrapper> current_chunk,
                                 ClientContext &context)
      : ArrowScanLocalState(std::move(current_chunk), context),
        filter_sel(STANDARD_VECTOR_SIZE) {}

  void *stream = nullptr;
  LanceSearchGlobalState *global_state = nullptr;
  bool filter_pushed_down = false;
  SelectionVector filter_sel;

  ~LanceSearchLocalState() override {
    if (stream) {
      lance_close_stream(stream);
    }
  }
};

static bool LanceSearchLoadNextBatch(ClientContext &context,
                                     LanceSearchLocalState &local_state,
                                     const LanceSearchBindData &bind_data,
                                     LanceSearchGlobalState &global) {
  if (!local_state.stream) {
#ifdef LANCE_VANE_DISTRIBUTED
    if (bind_data.vane_state.worker_bind) {
      auto &search = bind_data.vane_state;
      const uint8_t *filter_ir = search.final_filter_ir.empty()
                                     ? nullptr
                                     : reinterpret_cast<const uint8_t *>(
                                           search.final_filter_ir.data());
      auto *namespace_filter_plan =
          search.namespace_filter_plan.empty()
              ? nullptr
              : reinterpret_cast<const uint8_t *>(
                    search.namespace_filter_plan.data());
      auto *index_plan =
          reinterpret_cast<const uint8_t *>(search.index_plan.data());
      auto create_stream = [&](const uint8_t *ir, size_t ir_len) -> void * {
        if (search.arguments.kind == LanceVaneSearchKind::FTS) {
          return lance_vane_create_fts_stream_ir(
              global.vane_dataset, search.dataset_generation_id.c_str(),
              search.arguments.text_column.c_str(),
              search.arguments.text_query.c_str(), search.arguments.k, ir,
              ir_len, namespace_filter_plan,
              search.namespace_filter_plan.size(),
              search.arguments.prefilter ? 1 : 0, index_plan,
              search.index_plan.size());
        }
        return lance_vane_create_hybrid_stream_ir(
            global.vane_dataset, search.dataset_generation_id.c_str(),
            search.arguments.vector_column.c_str(),
            search.arguments.vector_query.data(),
            search.arguments.vector_query.size(),
            search.arguments.text_column.c_str(),
            search.arguments.text_query.c_str(), search.arguments.k,
            search.arguments.nprobes, search.arguments.refine_factor, ir,
            ir_len, namespace_filter_plan, search.namespace_filter_plan.size(),
            search.arguments.prefilter ? 1 : 0,
            search.arguments.use_index ? 1 : 0, search.arguments.alpha,
            search.arguments.oversample_factor, index_plan,
            search.index_plan.size());
      };
      local_state.stream =
          create_stream(filter_ir, search.final_filter_ir.size());
      if (!local_state.stream && filter_ir && !search.arguments.prefilter) {
        global.filter_pushdown_fallbacks.fetch_add(1);
        global.filter_pushed_down = false;
        local_state.filter_pushed_down = false;
        local_state.stream = create_stream(nullptr, 0);
      }
      if (!local_state.stream) {
        throw IOException(
            "Failed to create exact distributed Lance search stream" +
            LanceVaneFormatErrorSuffix(search.physical_uri,
                                       search.private_uri_diagnostics));
      }
    } else
#endif
        if (bind_data.namespace_backed) {
      vector<const char *> option_key_ptrs;
      vector<const char *> option_value_ptrs;
      vector<const char *> column_ptrs;
      string bearer_token;
      string api_key;
      LanceNamespaceQueryConfig config;
      FillLanceNamespaceQueryConfig(
          context, bind_data.namespace_config, bind_data.k, bind_data.prefilter,
          bind_data.namespace_filter, global.namespace_columns, option_key_ptrs,
          option_value_ptrs, column_ptrs, bearer_token, api_key, config);
      LanceNamespaceFtsSearchOptions options;
      options.text_column = bind_data.text_column.c_str();
      options.query = bind_data.query.c_str();
      local_state.stream =
          lance_create_namespace_fts_search_stream(&config, &options);
      if (!local_state.stream) {
#ifdef LANCE_VANE_DISTRIBUTED
        throw IOException(
            "Failed to create Lance namespace FTS stream" +
            LanceVaneFormatErrorSuffix(bind_data.file_path,
                                       bind_data.private_uri_diagnostics));
#else
        throw IOException("Failed to create Lance namespace FTS stream" +
                          LanceFormatErrorSuffix());
#endif
      }
    } else {
      const uint8_t *filter_ir = global.lance_filter_ir.empty()
                                     ? nullptr
                                     : reinterpret_cast<const uint8_t *>(
                                           global.lance_filter_ir.data());
      auto filter_ir_len = NumericCast<idx_t>(global.lance_filter_ir.size());

      auto create_stream = [&](const uint8_t *ir, idx_t ir_len) -> void * {
        if (bind_data.mode == LanceSearchMode::Fts) {
          return lance_create_fts_stream_ir(
              bind_data.dataset, bind_data.text_column.c_str(),
              bind_data.query.c_str(), bind_data.k, ir,
              NumericCast<size_t>(ir_len), bind_data.prefilter ? 1 : 0);
        }
        return lance_create_hybrid_stream_ir(
            bind_data.dataset, bind_data.vector_column.c_str(),
            bind_data.vector_query.data(), bind_data.vector_query.size(),
            bind_data.text_column.c_str(), bind_data.text_query.c_str(),
            bind_data.k, bind_data.nprobes, bind_data.refine_factor, ir,
            NumericCast<size_t>(ir_len), bind_data.prefilter ? 1 : 0,
            bind_data.use_index ? 1 : 0, bind_data.alpha,
            bind_data.oversample_factor);
      };

      local_state.stream = create_stream(filter_ir, filter_ir_len);
      if (!local_state.stream && filter_ir && !bind_data.prefilter) {
        // Best-effort: if filter pushdown failed, retry without it and rely on
        // DuckDB-side filter execution for correctness.
        global.filter_pushdown_fallbacks.fetch_add(1);
        global.filter_pushed_down = false;
        local_state.filter_pushed_down = false;
        local_state.stream = create_stream(nullptr, 0);
      }
      if (!local_state.stream) {
#ifdef LANCE_VANE_DISTRIBUTED
        throw IOException(
            "Failed to create Lance search stream" +
            LanceVaneFormatErrorSuffix(bind_data.file_path,
                                       bind_data.private_uri_diagnostics));
#else
        throw IOException("Failed to create Lance search stream" +
                          LanceFormatErrorSuffix());
#endif
      }
    }
  }

  void *batch = nullptr;
  auto rc = lance_stream_next(local_state.stream, &batch);
  if (rc == 1) {
    lance_close_stream(local_state.stream);
    local_state.stream = nullptr;
    return false;
  }
  if (rc != 0) {
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException(
        "Failed to read next Lance RecordBatch" +
        LanceVaneFormatErrorSuffix(bind_data.file_path,
                                   bind_data.private_uri_diagnostics));
#else
    throw IOException("Failed to read next Lance RecordBatch" +
                      LanceFormatErrorSuffix());
#endif
  }

  auto new_chunk = make_shared_ptr<ArrowArrayWrapper>();
  memset(&new_chunk->arrow_array, 0, sizeof(new_chunk->arrow_array));
  ArrowSchema tmp_schema;
  memset(&tmp_schema, 0, sizeof(tmp_schema));

  if (lance_batch_to_arrow(batch, &new_chunk->arrow_array, &tmp_schema) != 0) {
    lance_free_batch(batch);
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException(
        "Failed to export Lance RecordBatch to Arrow C Data Interface" +
        LanceVaneFormatErrorSuffix(bind_data.file_path,
                                   bind_data.private_uri_diagnostics));
#else
    throw IOException(
        "Failed to export Lance RecordBatch to Arrow C Data Interface" +
        LanceFormatErrorSuffix());
#endif
  }
  lance_free_batch(batch);

  // Widen Float16 columns before DuckDB consumes the batch.
  LanceCoerceArrowArrayForDuckDB(&tmp_schema, &new_chunk->arrow_array);

  local_state.global_state->record_batches.fetch_add(1);
  auto rows = NumericCast<idx_t>(new_chunk->arrow_array.length);
  local_state.global_state->record_batch_rows.fetch_add(rows);

  if (tmp_schema.release) {
    tmp_schema.release(&tmp_schema);
  }

  local_state.chunk = std::move(new_chunk);
  local_state.Reset();
  return true;
}

static unique_ptr<FunctionData> LanceFtsBind(ClientContext &context,
                                             TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types,
                                             vector<string> &names) {
  if (input.inputs.size() < 3) {
    throw InvalidInputException(
        "lance_fts requires (path, text_column, query)");
  }
  if (input.inputs[0].IsNull()) {
    throw InvalidInputException("lance_fts requires a dataset root path");
  }
  if (input.inputs[1].IsNull()) {
    throw InvalidInputException("lance_fts requires a non-null text_column");
  }
  if (input.inputs[2].IsNull()) {
    throw InvalidInputException("lance_fts requires a non-null query");
  }

  auto result = make_uniq<LanceSearchBindData>();
  result->mode = LanceSearchMode::Fts;
#ifdef LANCE_VANE_DISTRIBUTED
  result->private_uri_diagnostics = LanceVanePathRequiresRedaction(
      context, input.inputs[0].GetValue<string>());
  result->vane_overload = LanceVaneSearchOverload::FTS;
#endif
  result->text_column = input.inputs[1].GetValue<string>();
  result->query = input.inputs[2].GetValue<string>();
  result->namespace_filter = ParseOptionalNamedString(input, "filter");

  int64_t k_val = 10;
  auto k_named = input.named_parameters.find("k");
  if (k_named != input.named_parameters.end() && !k_named->second.IsNull()) {
    k_val =
        k_named->second.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
  }
  if (k_val <= 0) {
    throw InvalidInputException("lance_fts requires k > 0");
  }
  result->k = NumericCast<uint64_t>(k_val);

  auto prefilter_named = input.named_parameters.find("prefilter");
  if (prefilter_named != input.named_parameters.end() &&
      !prefilter_named->second.IsNull()) {
    result->prefilter =
        prefilter_named->second.DefaultCastAs(LogicalType::BOOLEAN)
            .GetValue<bool>();
  }

  if (auto *table =
          TryResolveNamespaceBackedSearchTable(context, input.inputs[0])) {
    result->namespace_backed = true;
    result->namespace_config = table->NamespaceConfig();
    result->file_path = table->DatasetUri();
#ifdef LANCE_VANE_DISTRIBUTED
    result->dataset_entry =
        LanceGetOrOpenDatasetEntryForTable(context, *table, result->file_path);
    result->dataset =
        result->dataset_entry ? result->dataset_entry->Handle() : nullptr;
    if (!result->dataset) {
      throw IOException(
          "Failed to open an exact Lance namespace search snapshot: " +
          LanceVaneTableDiagnosticPath(*table, result->file_path) +
          LanceVaneTableFormatErrorSuffix(*table, result->file_path));
    }
    auto snapshot_version = lance_dataset_version(result->dataset);
    if (snapshot_version == 0 ||
        snapshot_version > NumericLimits<int64_t>::Maximum()) {
      throw IOException(
          "Failed to resolve an exact Lance namespace search version" +
          LanceVaneTableFormatErrorSuffix(*table, result->file_path));
    }
    result->namespace_config.snapshot_version = snapshot_version;
    result->private_uri_diagnostics =
        LanceVaneTablePathRequiresRedaction(*table, result->file_path);
#endif
    result->text_column = RequireNamespaceSearchColumn(
        *table, result->text_column, "lance_fts", "text_column");
    if (result->prefilter && result->namespace_filter.empty()) {
      throw InvalidInputException(
          "lance_fts requires explicit filter when prefilter=true on "
          "namespace-backed tables");
    }
    PopulateNamespaceSearchSchema(
        context, *table, "_score", result->schema_root, result->arrow_table,
        result->names, result->types, names, return_types);
#ifdef LANCE_VANE_DISTRIBUTED
    PrepareVaneSearchCandidate(
        context, input.inputs[0], result->dataset_entry, result->file_path,
        result->private_uri_diagnostics, result->vane_candidate);
    LancePrepareSharedVaneState(*result);
#endif
    return std::move(result);
  }

  if (!result->namespace_filter.empty()) {
    throw InvalidInputException(
        "lance_fts filter parameter is only supported for namespace-backed "
        "tables");
  }

  result->file_path.clear();
  result->dataset_entry =
      OpenSearchDatasetEntry(context, input.inputs[0], "lance_fts",
                             result->file_path, &result->dataset_cache_hit);
#ifdef LANCE_VANE_DISTRIBUTED
  if (auto *table = TryResolveLanceTableEntry(
          context, input.inputs[0].GetValue<string>())) {
    result->private_uri_diagnostics =
        LanceVaneTablePathRequiresRedaction(*table, result->file_path);
  } else {
    result->private_uri_diagnostics =
        result->private_uri_diagnostics ||
        LanceVanePathRequiresRedaction(context, result->file_path);
  }
#endif
  result->dataset =
      result->dataset_entry ? result->dataset_entry->Handle() : nullptr;

  if (!result->dataset) {
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException("Failed to open Lance dataset: " +
                      LanceVaneDiagnosticPath(result->file_path,
                                              result->private_uri_diagnostics) +
                      LanceVaneFormatErrorSuffix(
                          result->file_path, result->private_uri_diagnostics));
#else
    throw IOException("Failed to open Lance dataset: " + result->file_path +
                      LanceFormatErrorSuffix());
#endif
  }

  auto *schema_handle = lance_get_fts_schema(
      result->dataset, result->text_column.c_str(), result->query.c_str(),
      result->k, result->prefilter ? 1 : 0);
  if (!schema_handle) {
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException("Failed to get Lance FTS schema: " +
                      LanceVaneDiagnosticPath(result->file_path,
                                              result->private_uri_diagnostics) +
                      LanceVaneFormatErrorSuffix(
                          result->file_path, result->private_uri_diagnostics));
#else
    throw IOException("Failed to get Lance FTS schema: " + result->file_path +
                      LanceFormatErrorSuffix());
#endif
  }

  memset(&result->schema_root.arrow_schema, 0,
         sizeof(result->schema_root.arrow_schema));
  if (lance_schema_to_arrow(schema_handle, &result->schema_root.arrow_schema) !=
      0) {
    lance_free_schema(schema_handle);
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException(
        "Failed to export Lance FTS schema to Arrow C Data Interface" +
        LanceVaneFormatErrorSuffix(result->file_path,
                                   result->private_uri_diagnostics));
#else
    throw IOException(
        "Failed to export Lance FTS schema to Arrow C Data Interface" +
        LanceFormatErrorSuffix());
#endif
  }
  lance_free_schema(schema_handle);
  LanceCoerceArrowSchemaForDuckDB(&result->schema_root.arrow_schema);
  ArrowTableFunction::PopulateArrowTableSchema(
      context, result->arrow_table, result->schema_root.arrow_schema);
  result->names = result->arrow_table.GetNames();
  result->types = result->arrow_table.GetTypes();
#ifdef LANCE_VANE_DISTRIBUTED
  PrepareVaneSearchCandidate(context, input.inputs[0], result->dataset_entry,
                             result->file_path, result->private_uri_diagnostics,
                             result->vane_candidate);
  LancePrepareSharedVaneState(*result);
#endif
  names = result->names;
  return_types = result->types;
  return std::move(result);
}

static unique_ptr<FunctionData>
LanceHybridBind(ClientContext &context, TableFunctionBindInput &input,
                vector<LogicalType> &return_types, vector<string> &names) {
  if (input.inputs.size() < 5) {
    throw InvalidInputException("lance_hybrid_search requires (path, "
                                "vector_column, vector, text_column, text)");
  }
  if (input.inputs[0].IsNull()) {
    throw InvalidInputException(
        "lance_hybrid_search requires a dataset root path");
  }
  if (input.inputs[1].IsNull()) {
    throw InvalidInputException(
        "lance_hybrid_search requires a non-null vector_column");
  }
  if (input.inputs[2].IsNull()) {
    throw InvalidInputException(
        "lance_hybrid_search requires a non-null query vector");
  }
  if (input.inputs[3].IsNull()) {
    throw InvalidInputException(
        "lance_hybrid_search requires a non-null text_column");
  }
  if (input.inputs[4].IsNull()) {
    throw InvalidInputException(
        "lance_hybrid_search requires a non-null query");
  }

  auto result = make_uniq<LanceSearchBindData>();
  result->mode = LanceSearchMode::Hybrid;
#ifdef LANCE_VANE_DISTRIBUTED
  result->private_uri_diagnostics = LanceVanePathRequiresRedaction(
      context, input.inputs[0].GetValue<string>());
  result->vane_overload = VaneVectorOverload(input.inputs[2].type(), true);
#endif
  result->file_path.clear();
  result->dataset_entry =
      OpenSearchDatasetEntry(context, input.inputs[0], "lance_hybrid_search",
                             result->file_path, &result->dataset_cache_hit);
#ifdef LANCE_VANE_DISTRIBUTED
  if (auto *table = TryResolveLanceTableEntry(
          context, input.inputs[0].GetValue<string>())) {
    result->private_uri_diagnostics =
        LanceVaneTablePathRequiresRedaction(*table, result->file_path);
  } else {
    result->private_uri_diagnostics =
        result->private_uri_diagnostics ||
        LanceVanePathRequiresRedaction(context, result->file_path);
  }
#endif
  result->dataset =
      result->dataset_entry ? result->dataset_entry->Handle() : nullptr;
  result->vector_column = input.inputs[1].GetValue<string>();
  result->vector_query =
      ParseQueryVector(input.inputs[2], "lance_hybrid_search");
  result->text_column = input.inputs[3].GetValue<string>();
  result->text_query = input.inputs[4].GetValue<string>();

  int64_t k_val = 10;
  auto k_named = input.named_parameters.find("k");
  if (k_named != input.named_parameters.end() && !k_named->second.IsNull()) {
    k_val =
        k_named->second.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
  }
  if (k_val <= 0) {
    throw InvalidInputException("lance_hybrid_search requires k > 0");
  }
  result->k = NumericCast<uint64_t>(k_val);

  bool has_nprobes = false;
  int64_t nprobes_val = 0;
  auto nprobes_named = input.named_parameters.find("nprobs");
  if (nprobes_named != input.named_parameters.end() &&
      !nprobes_named->second.IsNull()) {
    has_nprobes = true;
    nprobes_val = nprobes_named->second.DefaultCastAs(LogicalType::BIGINT)
                      .GetValue<int64_t>();
  }
  if (has_nprobes && nprobes_val <= 0) {
    throw InvalidInputException("lance_hybrid_search requires nprobs > 0");
  }
  result->nprobes = has_nprobes ? NumericCast<uint64_t>(nprobes_val) : 0;

  bool has_refine_factor = false;
  int64_t refine_factor_val = 0;
  auto refine_factor_named = input.named_parameters.find("refine_factor");
  if (refine_factor_named != input.named_parameters.end() &&
      !refine_factor_named->second.IsNull()) {
    has_refine_factor = true;
    refine_factor_val =
        refine_factor_named->second.DefaultCastAs(LogicalType::BIGINT)
            .GetValue<int64_t>();
  }
  if (has_refine_factor && refine_factor_val <= 0) {
    throw InvalidInputException(
        "lance_hybrid_search requires refine_factor > 0");
  }
  result->refine_factor =
      has_refine_factor ? NumericCast<uint64_t>(refine_factor_val) : 0;

  auto prefilter_named = input.named_parameters.find("prefilter");
  if (prefilter_named != input.named_parameters.end() &&
      !prefilter_named->second.IsNull()) {
    result->prefilter =
        prefilter_named->second.DefaultCastAs(LogicalType::BOOLEAN)
            .GetValue<bool>();
  }
  auto use_index_named = input.named_parameters.find("use_index");
  if (use_index_named != input.named_parameters.end() &&
      !use_index_named->second.IsNull()) {
    result->use_index =
        use_index_named->second.DefaultCastAs(LogicalType::BOOLEAN)
            .GetValue<bool>();
  }

  auto alpha_named = input.named_parameters.find("alpha");
  if (alpha_named != input.named_parameters.end() &&
      !alpha_named->second.IsNull()) {
    result->alpha =
        alpha_named->second.DefaultCastAs(LogicalType::FLOAT).GetValue<float>();
  }
  auto oversample_named = input.named_parameters.find("oversample_factor");
  if (oversample_named != input.named_parameters.end() &&
      !oversample_named->second.IsNull()) {
    auto v = oversample_named->second.DefaultCastAs(LogicalType::INTEGER)
                 .GetValue<int32_t>();
    if (v > 0) {
      result->oversample_factor = NumericCast<uint32_t>(v);
    }
  }

  if (!result->dataset) {
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException("Failed to open Lance dataset: " +
                      LanceVaneDiagnosticPath(result->file_path,
                                              result->private_uri_diagnostics) +
                      LanceVaneFormatErrorSuffix(
                          result->file_path, result->private_uri_diagnostics));
#else
    throw IOException("Failed to open Lance dataset: " + result->file_path +
                      LanceFormatErrorSuffix());
#endif
  }

  auto *schema_handle = lance_get_hybrid_schema(result->dataset);
  if (!schema_handle) {
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException("Failed to get Lance hybrid schema: " +
                      LanceVaneDiagnosticPath(result->file_path,
                                              result->private_uri_diagnostics) +
                      LanceVaneFormatErrorSuffix(
                          result->file_path, result->private_uri_diagnostics));
#else
    throw IOException("Failed to get Lance hybrid schema: " +
                      result->file_path + LanceFormatErrorSuffix());
#endif
  }

  memset(&result->schema_root.arrow_schema, 0,
         sizeof(result->schema_root.arrow_schema));
  if (lance_schema_to_arrow(schema_handle, &result->schema_root.arrow_schema) !=
      0) {
    lance_free_schema(schema_handle);
#ifdef LANCE_VANE_DISTRIBUTED
    throw IOException(
        "Failed to export Lance hybrid schema to Arrow C Data Interface" +
        LanceVaneFormatErrorSuffix(result->file_path,
                                   result->private_uri_diagnostics));
#else
    throw IOException(
        "Failed to export Lance hybrid schema to Arrow C Data Interface" +
        LanceFormatErrorSuffix());
#endif
  }
  lance_free_schema(schema_handle);
  LanceCoerceArrowSchemaForDuckDB(&result->schema_root.arrow_schema);
  ArrowTableFunction::PopulateArrowTableSchema(
      context, result->arrow_table, result->schema_root.arrow_schema);
  result->names = result->arrow_table.GetNames();
  result->types = result->arrow_table.GetTypes();
#ifdef LANCE_VANE_DISTRIBUTED
  PrepareVaneSearchCandidate(context, input.inputs[0], result->dataset_entry,
                             result->file_path, result->private_uri_diagnostics,
                             result->vane_candidate);
  LancePrepareSharedVaneState(*result);
#endif
  names = result->names;
  return_types = result->types;
  return std::move(result);
}

static unique_ptr<GlobalTableFunctionState>
LanceSearchInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<LanceSearchBindData>();
  auto state =
      make_uniq_base<GlobalTableFunctionState, LanceSearchGlobalState>();
  auto &global = state->Cast<LanceSearchGlobalState>();

  global.projection_ids = input.projection_ids;
  if (!input.projection_ids.empty()) {
    global.scanned_types.reserve(input.column_ids.size());
    for (auto col_id : input.column_ids) {
      if (col_id >= bind_data.types.size()) {
        throw IOException("Invalid column id in projection");
      }
      global.scanned_types.push_back(bind_data.types[col_id]);
    }
  }

#ifdef LANCE_VANE_DISTRIBUTED
  if (bind_data.vane_state.worker_bind) {
    LanceVaneValidateExecutionInput(input, bind_data.vane_state);
    global.vane_dataset_entry =
        LanceVaneOpenSearchSnapshot(context, bind_data.vane_state);
    global.vane_dataset = global.vane_dataset_entry->Handle();
    global.lance_filter_ir = bind_data.vane_state.final_filter_ir;
    global.filter_pushed_down = bind_data.vane_state.filter_pushed_down;
    return state;
  }
#endif

  if (bind_data.namespace_backed) {
    return state;
  }

  auto table_filters = BuildLanceTableFilterIRParts(
      bind_data.names, bind_data.types, input, true);
  if (bind_data.prefilter && !table_filters.all_prefilterable_filters_pushed) {
    auto function_name = bind_data.mode == LanceSearchMode::Fts
                             ? "lance_fts"
                             : "lance_hybrid_search";
    throw InvalidInputException(string(function_name) +
                                " requires filter pushdown for prefilterable "
                                "columns when prefilter=true");
  }

  bool has_table_filter_parts = !table_filters.parts.empty();
  string filter_ir_msg;
  if (!table_filters.parts.empty()) {
    if (!TryEncodeLanceFilterIRMessage(table_filters.parts, filter_ir_msg)) {
      filter_ir_msg.clear();
    }
    global.lance_filter_ir = std::move(filter_ir_msg);
  }
  if (bind_data.prefilter && has_table_filter_parts &&
      global.lance_filter_ir.empty()) {
    throw IOException("Failed to encode Lance filter IR");
  }
  global.filter_pushed_down =
      table_filters.all_filters_pushed && !global.lance_filter_ir.empty();
  return state;
}

static unique_ptr<LocalTableFunctionState>
LanceSearchLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                     GlobalTableFunctionState *global_state) {
  auto &global = global_state->Cast<LanceSearchGlobalState>();

  auto chunk = make_uniq<ArrowArrayWrapper>();
  auto result =
      make_uniq<LanceSearchLocalState>(std::move(chunk), context.client);
  result->column_ids = input.column_ids;
  result->filters = input.filters.get();
  result->global_state = &global;
  result->filter_pushed_down = global.filter_pushed_down;
  if (global.CanRemoveFilterColumns()) {
    result->all_columns.Initialize(context.client, global.scanned_types);
  }
  return std::move(result);
}

static void LanceSearchFunc(ClientContext &context, TableFunctionInput &data,
                            DataChunk &output) {
  if (!data.local_state) {
    return;
  }

  auto &bind_data = data.bind_data->Cast<LanceSearchBindData>();
  auto &global_state = data.global_state->Cast<LanceSearchGlobalState>();
  auto &local_state = data.local_state->Cast<LanceSearchLocalState>();

  while (true) {
    if (local_state.chunk_offset >=
        NumericCast<idx_t>(local_state.chunk->arrow_array.length)) {
      if (!LanceSearchLoadNextBatch(context, local_state, bind_data,
                                    global_state)) {
        return;
      }
    }

    auto remaining = NumericCast<idx_t>(local_state.chunk->arrow_array.length) -
                     local_state.chunk_offset;
    auto output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
    global_state.lines_read.fetch_add(output_size);

    if (global_state.CanRemoveFilterColumns()) {
      local_state.all_columns.Reset();
      local_state.all_columns.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(local_state,
                                        bind_data.arrow_table.GetColumns(),
                                        local_state.all_columns, false);
      local_state.chunk_offset += output_size;
      if (local_state.filters && !local_state.filter_pushed_down) {
        ApplyDuckDBFilters(context, *local_state.filters,
                           local_state.all_columns, local_state.filter_sel);
      }
      output.ReferenceColumns(local_state.all_columns,
                              global_state.projection_ids);
      output.SetCardinality(local_state.all_columns);
    } else {
      output.SetCardinality(output_size);
      ArrowTableFunction::ArrowToDuckDB(
          local_state, bind_data.arrow_table.GetColumns(), output, false);
      local_state.chunk_offset += output_size;
      if (local_state.filters && !local_state.filter_pushed_down) {
        ApplyDuckDBFilters(context, *local_state.filters, output,
                           local_state.filter_sel);
      }
    }

    if (output.size() == 0) {
      continue;
    }
    output.Verify();
    return;
  }
}

static InsertionOrderPreservingMap<string>
LanceSearchBindToString(const LanceSearchBindData &bind_data) {
  InsertionOrderPreservingMap<string> result;
#ifdef LANCE_VANE_DISTRIBUTED
  result["Lance Path"] = LanceVaneDiagnosticPath(
      bind_data.file_path, bind_data.private_uri_diagnostics);
#else
  result["Lance Path"] = bind_data.file_path;
#endif
  result["Lance Search Backend"] =
      bind_data.namespace_backed ? "namespace_query_table" : "dataset_scan";
  result["Lance Search Mode"] =
      bind_data.mode == LanceSearchMode::Fts ? "fts" : "hybrid";
  result["Lance K"] = to_string(bind_data.k);
  result["Lance Prefilter"] = bind_data.prefilter ? "true" : "false";
  result["Lance Dataset Cache Hit"] =
      bind_data.dataset_cache_hit ? "true" : "false";
  if (!bind_data.namespace_filter.empty()) {
    result["Lance Namespace Filter"] = bind_data.namespace_filter;
  }

  if (bind_data.mode == LanceSearchMode::Fts) {
    result["Lance Text Column"] = bind_data.text_column;
    result["Lance Query"] = bind_data.query;
  } else {
    result["Lance Vector Column"] = bind_data.vector_column;
    result["Lance Text Column"] = bind_data.text_column;
    result["Lance Vector Query Dim"] = to_string(bind_data.vector_query.size());
    result["Lance Text Query"] = bind_data.text_query;
    result["Lance Nprobes"] = to_string(bind_data.nprobes);
    result["Lance Refine Factor"] = to_string(bind_data.refine_factor);
    result["Lance Use Index"] = bind_data.use_index ? "true" : "false";
    result["Lance Alpha"] = to_string(bind_data.alpha);
    result["Lance Oversample Factor"] = to_string(bind_data.oversample_factor);
  }

  return result;
}

static InsertionOrderPreservingMap<string>
LanceSearchToString(TableFunctionToStringInput &input) {
  auto &bind_data = input.bind_data->Cast<LanceSearchBindData>();
  return LanceSearchBindToString(bind_data);
}

static InsertionOrderPreservingMap<string>
LanceSearchDynamicToString(TableFunctionDynamicToStringInput &input) {
  auto &bind_data = input.bind_data->Cast<LanceSearchBindData>();
  auto result = LanceSearchBindToString(bind_data);
  auto &global_state = input.global_state->Cast<LanceSearchGlobalState>();

  result["Lance Filter Pushed Down"] =
      global_state.filter_pushed_down ? "true" : "false";
  result["Lance Filter Pushdown Fallbacks"] =
      to_string(global_state.filter_pushdown_fallbacks.load());
  result["Lance Filter IR Bytes"] =
      to_string(global_state.lance_filter_ir.size());
  result["Lance Record Batches"] =
      to_string(global_state.record_batches.load());
  result["Lance Record Batch Rows"] =
      to_string(global_state.record_batch_rows.load());
  result["Lance Rows Out"] = to_string(global_state.lines_read.load());

  return result;
}

#ifdef LANCE_VANE_DISTRIBUTED
static LanceVaneSearchArguments
LanceSharedVaneArguments(const LanceSearchBindData &bind_data) {
  LanceVaneSearchArguments result;
  result.kind = bind_data.mode == LanceSearchMode::Fts
                    ? LanceVaneSearchKind::FTS
                    : LanceVaneSearchKind::HYBRID;
  result.overload = bind_data.vane_overload;
  result.vector_column = bind_data.vector_column;
  result.vector_query = bind_data.vector_query;
  result.text_column = bind_data.text_column;
  result.text_query = bind_data.mode == LanceSearchMode::Fts
                          ? bind_data.query
                          : bind_data.text_query;
  result.k = bind_data.k;
  result.nprobes = bind_data.nprobes;
  result.refine_factor = bind_data.refine_factor;
  result.prefilter = bind_data.prefilter;
  result.use_index = bind_data.use_index;
  result.namespace_backed = bind_data.namespace_backed;
  result.alpha = bind_data.alpha;
  result.oversample_factor = bind_data.oversample_factor;
  result.namespace_filter = bind_data.namespace_filter;
  return result;
}

static void LancePrepareSharedVaneState(LanceSearchBindData &bind_data) {
  bind_data.vane_state = LanceVanePrepareGlobalSearchState(
      bind_data.vane_candidate, LanceSharedVaneArguments(bind_data),
      bind_data.names, bind_data.types);
}

static LanceVaneGlobalSearchState
LanceBuildSharedVaneState(const TableFunctionDistributedScanInput &input,
                          const LanceSearchBindData &bind_data) {
  if (bind_data.vane_state.worker_bind) {
    LanceVaneValidateDistributedInput(input, bind_data.vane_state);
    return bind_data.vane_state;
  }
  return LanceVaneFinalizeGlobalSearchState(
      input, bind_data.vane_state, bind_data.lance_pushed_filter_ir_parts,
      bind_data.complex_filter_pushdown_failed);
}

static vector<DistributedScanSplit> LancePlanDistributedSharedSearch(
    const TableFunctionDistributedScanPlanningInput &input) {
  auto &bind_data = input.bind_data->Cast<LanceSearchBindData>();
  auto state = LanceBuildSharedVaneState(input, bind_data);
  return LanceVaneCreateSearchTaskAssignments(state);
}

static unique_ptr<FunctionData> LanceCreateDistributedSharedWorkerBind(
    const TableFunctionDistributedScanInput &input) {
  auto &source = input.bind_data->Cast<LanceSearchBindData>();
  auto state = LanceBuildSharedVaneState(input, source);
  LanceVanePrepareSearchWorkerBindState(state);

  auto result = make_uniq<LanceSearchBindData>();
  result->column_ids = source.column_ids;
  result->mode = state.arguments.kind == LanceVaneSearchKind::FTS
                     ? LanceSearchMode::Fts
                     : LanceSearchMode::Hybrid;
  result->file_path = state.physical_uri;
  result->private_uri_diagnostics = state.private_uri_diagnostics;
  result->prefilter = state.arguments.prefilter;
  result->namespace_backed = false;
  result->namespace_filter = state.arguments.namespace_filter;
  result->text_column = state.arguments.text_column;
  result->query = state.arguments.text_query;
  result->vector_column = state.arguments.vector_column;
  result->vector_query = state.arguments.vector_query;
  result->text_query = state.arguments.text_query;
  result->nprobes = state.arguments.nprobes;
  result->refine_factor = state.arguments.refine_factor;
  result->use_index = state.arguments.use_index;
  result->alpha = state.arguments.alpha;
  result->oversample_factor = state.arguments.oversample_factor;
  result->k = state.arguments.k;
  result->names = state.output_names;
  result->types = state.output_types;
  result->vane_overload = state.arguments.overload;
  result->vane_state = std::move(state);
  return result;
}

static void
LanceApplyDistributedSharedSearch(optional_ptr<FunctionData> worker_bind,
                                  const vector<DistributedScanSplit> &splits) {
  auto &bind_data = worker_bind->Cast<LanceSearchBindData>();
  LanceVaneApplySearchTaskAssignments(bind_data.vane_state, splits);
}

static void LanceSearchSerialize(Serializer &serializer,
                                 const optional_ptr<FunctionData> bind_data,
                                 const TableFunction &) {
  auto &data = bind_data->Cast<LanceSearchBindData>();
  auto state = data.vane_state;
  if (!state.finalized) {
    LanceVaneAccumulatePendingGlobalSearchFilters(
        state, data.lance_pushed_filter_ir_parts,
        data.complex_filter_pushdown_failed);
  }
  LanceVaneSerializeGlobalSearchState(serializer, state);
}

static unique_ptr<FunctionData>
LanceSearchDeserialize(Deserializer &deserializer, TableFunction &) {
  auto state = LanceVaneDeserializeGlobalSearchState(deserializer);
  if (state.arguments.kind == LanceVaneSearchKind::VECTOR ||
      state.arguments.overload == LanceVaneSearchOverload::VECTOR_FLOAT ||
      state.arguments.overload == LanceVaneSearchOverload::VECTOR_DOUBLE ||
      (state.arguments.kind == LanceVaneSearchKind::FTS &&
       state.arguments.overload != LanceVaneSearchOverload::FTS) ||
      (state.arguments.kind == LanceVaneSearchKind::HYBRID &&
       state.arguments.overload != LanceVaneSearchOverload::HYBRID_FLOAT &&
       state.arguments.overload != LanceVaneSearchOverload::HYBRID_DOUBLE)) {
    throw SerializationException(
        "Distributed Lance search overload identity mismatch");
  }
  auto result = make_uniq<LanceSearchBindData>();
  result->mode = state.arguments.kind == LanceVaneSearchKind::FTS
                     ? LanceSearchMode::Fts
                     : LanceSearchMode::Hybrid;
  result->file_path = state.physical_uri;
  result->private_uri_diagnostics = state.private_uri_diagnostics;
  result->prefilter = state.arguments.prefilter;
  result->namespace_filter = state.arguments.namespace_filter;
  result->text_column = state.arguments.text_column;
  result->query = state.arguments.text_query;
  result->vector_column = state.arguments.vector_column;
  result->vector_query = state.arguments.vector_query;
  result->text_query = state.arguments.text_query;
  result->nprobes = state.arguments.nprobes;
  result->refine_factor = state.arguments.refine_factor;
  result->use_index = state.arguments.use_index;
  result->alpha = state.arguments.alpha;
  result->oversample_factor = state.arguments.oversample_factor;
  result->k = state.arguments.k;
  result->names = state.output_names;
  result->types = state.output_types;
  result->vane_overload = state.arguments.overload;
  result->vane_state = std::move(state);
  auto &context = deserializer.Get<ClientContext &>();
  LanceVanePopulateSearchSchema(context, result->names, result->types,
                                result->schema_root, result->arrow_table);
  return result;
}

static TableFunctionDistributedScanCallbacks
LanceSharedDistributedSearchCallbacks() {
  return LanceVaneSearchTaskCallbacks(LancePlanDistributedSharedSearch,
                                      LanceCreateDistributedSharedWorkerBind,
                                      LanceApplyDistributedSharedSearch);
}
#endif

static void RegisterLanceFtsSearch(ExtensionLoader &loader) {
  TableFunction fts(
      "lance_fts",
      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
      LanceSearchFunc, LanceFtsBind, LanceSearchInitGlobal,
      LanceSearchLocalInit);
  fts.named_parameters["k"] = LogicalType::BIGINT;
  fts.named_parameters["prefilter"] = LogicalType::BOOLEAN;
  fts.named_parameters["filter"] = LogicalType::VARCHAR;
  fts.projection_pushdown = true;
  fts.filter_pushdown = true;
  fts.filter_prune = true;
  fts.pushdown_expression = LancePushdownExpression;
#ifdef LANCE_VANE_DISTRIBUTED
  fts.pushdown_complex_filter = LanceSearchPushdownComplexFilter;
  fts.serialize = LanceSearchSerialize;
  fts.deserialize = LanceSearchDeserialize;
  fts.SetDistributedScanCallbacks(LanceSharedDistributedSearchCallbacks());
#endif
  fts.to_string = LanceSearchToString;
  fts.dynamic_to_string = LanceSearchDynamicToString;
  loader.RegisterFunction(fts);
}

static void RegisterLanceHybridSearch(ExtensionLoader &loader) {
  auto configure = [](TableFunction &fun) {
    fun.named_parameters["k"] = LogicalType::BIGINT;
    fun.named_parameters["nprobs"] = LogicalType::BIGINT;
    fun.named_parameters["refine_factor"] = LogicalType::BIGINT;
    fun.named_parameters["prefilter"] = LogicalType::BOOLEAN;
    fun.named_parameters["use_index"] = LogicalType::BOOLEAN;
    fun.named_parameters["alpha"] = LogicalType::FLOAT;
    fun.named_parameters["oversample_factor"] = LogicalType::INTEGER;
    fun.projection_pushdown = true;
    fun.filter_pushdown = true;
    fun.filter_prune = true;
    fun.pushdown_expression = LancePushdownExpression;
#ifdef LANCE_VANE_DISTRIBUTED
    fun.pushdown_complex_filter = LanceSearchPushdownComplexFilter;
    fun.serialize = LanceSearchSerialize;
    fun.deserialize = LanceSearchDeserialize;
    fun.SetDistributedScanCallbacks(LanceSharedDistributedSearchCallbacks());
#endif
    fun.to_string = LanceSearchToString;
    fun.dynamic_to_string = LanceSearchDynamicToString;
  };

  TableFunction hybrid_f32("lance_hybrid_search",
                           {LogicalType::VARCHAR, LogicalType::VARCHAR,
                            LogicalType::LIST(LogicalType::FLOAT),
                            LogicalType::VARCHAR, LogicalType::VARCHAR},
                           LanceSearchFunc, LanceHybridBind,
                           LanceSearchInitGlobal, LanceSearchLocalInit);
  configure(hybrid_f32);
  loader.RegisterFunction(hybrid_f32);

  TableFunction hybrid_f64("lance_hybrid_search",
                           {LogicalType::VARCHAR, LogicalType::VARCHAR,
                            LogicalType::LIST(LogicalType::DOUBLE),
                            LogicalType::VARCHAR, LogicalType::VARCHAR},
                           LanceSearchFunc, LanceHybridBind,
                           LanceSearchInitGlobal, LanceSearchLocalInit);
  configure(hybrid_f64);
  loader.RegisterFunction(hybrid_f64);
}

void RegisterLanceSearch(ExtensionLoader &loader) {
  RegisterLanceVectorSearch(loader);
  RegisterLanceFtsSearch(loader);
  RegisterLanceHybridSearch(loader);
}

#ifdef LANCE_VANE_DISTRIBUTED
void RegisterLanceSearchOptimizer(DBConfig &config) {
  OptimizerExtension extension;
  extension.optimize_function = LanceExactVectorCandidatesOptimizer;
  OptimizerExtension::Register(config, std::move(extension));
}
#endif

} // namespace duckdb
