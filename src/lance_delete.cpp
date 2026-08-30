#include "duckdb.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#ifdef LANCE_VANE_DISTRIBUTED
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#endif
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include "lance_common.hpp"
#include "lance_dataset_cache.hpp"
#include "lance_delete.hpp"
#ifdef LANCE_VANE_DISTRIBUTED
#include "lance_distributed_write.hpp"
#endif
#include "lance_ffi.hpp"
#include "lance_filter_ir.hpp"
#include "lance_insert.hpp"
#ifdef LANCE_VANE_DISTRIBUTED
#include "lance_scan_bind_data.hpp"
#endif
#include "lance_session_state.hpp"
#include "lance_table_entry.hpp"

namespace duckdb {

static bool TryCollectDeleteFilterPredicates(LogicalOperator &node,
                                             vector<const Expression *> &out,
                                             string &out_error) {
  switch (node.type) {
  case LogicalOperatorType::LOGICAL_FILTER: {
    auto &filter = node.Cast<LogicalFilter>();
    for (auto &expr : filter.expressions) {
      out.push_back(expr.get());
    }
    break;
  }
  case LogicalOperatorType::LOGICAL_PROJECTION:
    break;
  case LogicalOperatorType::LOGICAL_GET:
    return true;
  default:
    out_error = "unsupported DELETE plan shape: " + node.GetName();
    return false;
  }

  if (node.children.size() != 1) {
    out_error = "unsupported DELETE plan: expected 1 child, got " +
                to_string(node.children.size());
    return false;
  }
  return TryCollectDeleteFilterPredicates(*node.children[0], out, out_error);
}

static bool TryFindDeleteLogicalGet(LogicalOperator &node, LogicalGet *&out_get,
                                    string &out_error) {
  if (node.type == LogicalOperatorType::LOGICAL_GET) {
    auto &get = node.Cast<LogicalGet>();
    if (get.function.name != "__lance_table_scan") {
      out_error = "unsupported DELETE plan: expected __lance_table_scan, got " +
                  get.function.name;
      return false;
    }
    out_get = &get;
    return true;
  }
  if (node.children.size() != 1) {
    out_error = "unsupported DELETE plan: expected 1 child, got " +
                to_string(node.children.size());
    return false;
  }
  return TryFindDeleteLogicalGet(*node.children[0], out_get, out_error);
}

static bool TryBuildLanceDeleteFilterIR(LogicalDelete &op,
                                        string &out_filter_ir,
                                        string &out_error) {
  out_filter_ir.clear();
  out_error.clear();

  if (op.children.size() != 1) {
    out_error = "unsupported DELETE plan: expected 1 child, got " +
                to_string(op.children.size());
    return false;
  }

  LogicalGet *get = nullptr;
  if (!TryFindDeleteLogicalGet(*op.children[0], get, out_error)) {
    return false;
  }
  if (!get) {
    out_error = "unsupported DELETE plan: missing LogicalGet";
    return false;
  }

  vector<string> names;
  vector<LogicalType> types;
  names.reserve(op.table.GetColumns().PhysicalColumnCount());
  types.reserve(op.table.GetColumns().PhysicalColumnCount());
  for (auto &col : op.table.GetColumns().Physical()) {
    names.push_back(col.Name());
    types.push_back(col.Type());
  }

  vector<string> parts;

  vector<ColumnIndex> column_indexes = get->GetColumnIds();
  TableFunctionInitInput init_input(get->bind_data.get(),
                                    std::move(column_indexes),
                                    get->projection_ids, &get->table_filters);
  auto table_filters =
      BuildLanceTableFilterIRParts(names, types, init_input, false);
  parts = std::move(table_filters.parts);

  vector<const Expression *> predicates;
  if (!TryCollectDeleteFilterPredicates(*op.children[0], predicates,
                                        out_error)) {
    return false;
  }
  parts.reserve(parts.size() + predicates.size());
  for (auto *predicate : predicates) {
    if (!predicate) {
      out_error = "unsupported DELETE plan: null predicate expression";
      return false;
    }
    string part;
    if (!TryBuildLanceExprFilterIR(*get, names, types, false, *predicate,
                                   part)) {
      out_error =
          "unsupported DELETE predicate for Lance: " + predicate->ToString();
      return false;
    }
    parts.push_back(std::move(part));
  }

  if (!TryEncodeLanceFilterIRMessage(parts, out_filter_ir)) {
    out_error = "failed to encode Lance filter IR for DELETE";
    return false;
  }

  return true;
}

struct LanceDeleteSourceState : public GlobalSourceState {
  bool emitted = false;
};

class PhysicalLanceDelete final : public PhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE =
      PhysicalOperatorType::EXTENSION;

#ifdef LANCE_VANE_DISTRIBUTED
  PhysicalLanceDelete(
      PhysicalPlan &physical_plan, vector<LogicalType> types_p,
      LanceTableEntry &table_p, string filter_ir_p,
      unique_ptr<LanceDistributedWriteProvider> distributed_write_p,
      idx_t estimated_cardinality)
      : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION,
                         std::move(types_p), estimated_cardinality),
        table(table_p), filter_ir(std::move(filter_ir_p)),
        distributed_write(std::move(distributed_write_p)) {}
#else
  PhysicalLanceDelete(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
                      LanceTableEntry &table_p, string filter_ir_p,
                      idx_t estimated_cardinality)
      : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION,
                         std::move(types_p), estimated_cardinality),
        table(table_p), filter_ir(std::move(filter_ir_p)) {}
#endif

  bool IsSource() const override { return true; }

  unique_ptr<GlobalSourceState>
  GetGlobalSourceState(ClientContext &) const override {
    return make_uniq<LanceDeleteSourceState>();
  }

  SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                   OperatorSourceInput &input) const override {
    auto &state = input.global_state.Cast<LanceDeleteSourceState>();
    if (state.emitted) {
      return SourceResultType::FINISHED;
    }
    state.emitted = true;

    string open_path;
    vector<string> option_keys;
    vector<string> option_values;
    string display_uri;
    ResolveLanceStorageOptionsForTable(context.client, table, open_path,
                                       option_keys, option_values, display_uri);

    vector<const char *> key_ptrs;
    vector<const char *> value_ptrs;
    BuildStorageOptionPointerArrays(option_keys, option_values, key_ptrs,
                                    value_ptrs);

    void *txn = nullptr;
    int64_t deleted_rows = 0;
    auto *filter_ptr =
        filter_ir.empty() ? nullptr
                          : reinterpret_cast<const uint8_t *>(filter_ir.data());
    auto rc = lance_delete_transaction_with_storage_options(
        open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
        value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
        filter_ptr, filter_ir.size(), LanceGetSessionHandle(context.client),
        &txn, &deleted_rows);
    if (rc != 0) {
      throw IOException("Failed to create Lance DELETE transaction for '" +
                        open_path + "'" + LanceFormatErrorSuffix());
    }
    if (!txn) {
      if (deleted_rows != 0) {
        throw IOException("Failed to create Lance DELETE transaction for '" +
                          open_path +
                          "': null transaction returned for non-zero deleted "
                          "rows");
      }
    } else if (context.client.transaction.IsAutoCommit()) {
      rc = lance_commit_transaction_with_storage_options(
          open_path.c_str(), key_ptrs.empty() ? nullptr : key_ptrs.data(),
          value_ptrs.empty() ? nullptr : value_ptrs.data(), option_keys.size(),
          LanceGetSessionHandle(context.client), txn);
      if (rc != 0) {
        throw IOException("Failed to commit Lance DELETE transaction for '" +
                          open_path + "'" + LanceFormatErrorSuffix());
      }
      LanceInvalidateDatasetCacheForTable(context.client, table);
    } else {
      auto cache_key = LanceBuildDatasetCacheKeyForTable(context.client, table);
      RegisterLancePendingAppend(context.client, table.catalog,
                                 std::move(open_path), std::move(option_keys),
                                 std::move(option_values), std::move(cache_key),
                                 txn);
    }

    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(deleted_rows));
    return SourceResultType::FINISHED;
  }

  string GetName() const override { return "LanceDelete"; }

#ifdef LANCE_VANE_DISTRIBUTED
  optional_ptr<distributed::ExtensionWriteTaskProvider>
  GetExtensionWriteTaskProvider() override {
    if (!distributed_write) {
      throw InternalException(
          "Distributed Lance DELETE has no write-task provider");
    }
    if (children.size() != 1) {
      throw InvalidInputException(
          "Distributed Lance DELETE requires exactly one physical child");
    }
    return distributed_write->Select();
  }

  vector<const_reference<PhysicalOperator>> GetSources() const override {
    return {*this};
  }

  void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override {
    if (distributed_write && distributed_write->DistributedPlanSelected()) {
      throw InvalidInputException(
          "A distributed Lance DELETE worker plan cannot execute as a native "
          "coordinator operator");
    }
    op_state.reset();
    meta_pipeline.GetState().SetPipelineSource(current, *this);
  }
#endif

private:
  LanceTableEntry &table;
  string filter_ir;
#ifdef LANCE_VANE_DISTRIBUTED
  unique_ptr<LanceDistributedWriteProvider> distributed_write;
#endif
};

PhysicalOperator &PlanLanceDelete(ClientContext &context,
                                  PhysicalPlanGenerator &planner,
                                  LogicalDelete &op) {
  if (op.return_chunk) {
    throw NotImplementedException(
        "Lance DELETE does not support RETURNING yet");
  }

  auto *lance_table = dynamic_cast<LanceTableEntry *>(&op.table);
  if (!lance_table) {
    throw InternalException("PlanLanceDelete called for non-Lance table");
  }

  string filter_ir;
  string error;
  if (!TryBuildLanceDeleteFilterIR(op, filter_ir, error)) {
    if (error.empty()) {
      error = "unsupported DELETE plan";
    }
    throw NotImplementedException(error);
  }

#ifdef LANCE_VANE_DISTRIBUTED
  LogicalGet *get = nullptr;
  if (!TryFindDeleteLogicalGet(*op.children[0], get, error) || !get ||
      !get->bind_data) {
    throw NotImplementedException(
        error.empty() ? "Lance DELETE has no replayable scan" : error);
  }
  auto *scan_bind = dynamic_cast<LanceScanBindData *>(get->bind_data.get());
  if (!scan_bind) {
    throw InternalException("Lance DELETE is missing Lance scan bind data");
  }
  if (op.expressions.size() != 1 || !op.expressions[0] ||
      op.expressions[0]->GetExpressionClass() != ExpressionClass::BOUND_REF) {
    throw NotImplementedException(
        "Distributed Lance DELETE requires one bound row-id expression");
  }
  const auto row_id_index = NumericCast<idx_t>(
      op.expressions[0]->Cast<BoundReferenceExpression>().index);
  auto &child = planner.CreatePlan(*op.children[0]);
  auto provider = CreateLanceDistributedDeleteProvider(
      context, *lance_table, *scan_bind, child.GetTypes(), row_id_index);
  auto &del = planner.Make<PhysicalLanceDelete>(
      op.types, *lance_table, std::move(filter_ir), std::move(provider),
      op.estimated_cardinality);
  del.children.push_back(child);
#else
  auto &del = planner.Make<PhysicalLanceDelete>(
      op.types, *lance_table, std::move(filter_ir), op.estimated_cardinality);
#endif
  (void)context;
  return del;
}

} // namespace duckdb
