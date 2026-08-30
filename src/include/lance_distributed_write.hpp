#pragma once

#ifdef LANCE_VANE_DISTRIBUTED

#include "duckdb/execution/distributed/extension_write_task_provider.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;
class LanceTableEntry;
class LogicalCreateTable;
class PhysicalPlanGenerator;
struct LanceScanBindData;

class LanceDistributedWriteProvider final
    : public distributed::ExtensionWriteTaskProvider {
public:
  ~LanceDistributedWriteProvider() override;

  optional_ptr<distributed::ExtensionWriteTaskProvider> Select();
  bool DistributedPlanSelected() const;

  const distributed::DistributedExtensionWritePlan &WritePlan() const override;
  void ValidateDistributedWrite(ClientContext &context) const override;
  void PrepareDistributedWrite(ClientContext &context) const override;
  idx_t FinalizeDistributedWrite(
      ClientContext &context,
      const vector<DistributedWriteTaskResult> &results) const override;
  void AbortDistributedWrite(ClientContext &context,
                             const vector<DistributedWriteTaskResult>
                                 &selected_results) const override;

private:
  class Impl;

  explicit LanceDistributedWriteProvider(unique_ptr<Impl> impl);

  unique_ptr<Impl> impl;

  friend unique_ptr<LanceDistributedWriteProvider>
  CreateLanceDistributedInsertProvider(ClientContext &context,
                                       LanceTableEntry &table,
                                       const vector<string> &column_names,
                                       const vector<LogicalType> &column_types,
                                       const vector<LogicalType> &input_types);
  friend unique_ptr<LanceDistributedWriteProvider>
  CreateLanceDistributedDeleteProvider(ClientContext &context,
                                       LanceTableEntry &table,
                                       const LanceScanBindData &scan_bind,
                                       const vector<LogicalType> &input_types,
                                       idx_t row_id_index);
  friend unique_ptr<LanceDistributedWriteProvider>
  CreateLanceDistributedUpdateProvider(ClientContext &context,
                                       LanceTableEntry &table,
                                       const LanceScanBindData &scan_bind,
                                       const vector<LogicalType> &input_types,
                                       idx_t row_id_index,
                                       const vector<string> &set_columns,
                                       const vector<string> &set_expr_irs);
  friend PhysicalOperator &PlanLanceDistributedCreateTableAs(
      ClientContext &context, PhysicalPlanGenerator &planner,
      LogicalCreateTable &op, PhysicalOperator &plan, const string &root,
      const vector<string> &attached_option_keys,
      const vector<string> &attached_option_values,
      bool uses_coordinator_storage_secret,
      bool distributed_replay_path_restricted,
      const string &data_storage_version);
};

unique_ptr<LanceDistributedWriteProvider> CreateLanceDistributedInsertProvider(
    ClientContext &context, LanceTableEntry &table,
    const vector<string> &column_names, const vector<LogicalType> &column_types,
    const vector<LogicalType> &input_types);

unique_ptr<LanceDistributedWriteProvider> CreateLanceDistributedDeleteProvider(
    ClientContext &context, LanceTableEntry &table,
    const LanceScanBindData &scan_bind, const vector<LogicalType> &input_types,
    idx_t row_id_index);

unique_ptr<LanceDistributedWriteProvider> CreateLanceDistributedUpdateProvider(
    ClientContext &context, LanceTableEntry &table,
    const LanceScanBindData &scan_bind, const vector<LogicalType> &input_types,
    idx_t row_id_index, const vector<string> &set_columns,
    const vector<string> &set_expr_irs);

PhysicalOperator &PlanLanceDistributedCreateTableAs(
    ClientContext &context, PhysicalPlanGenerator &planner,
    LogicalCreateTable &op, PhysicalOperator &plan, const string &root,
    const vector<string> &attached_option_keys,
    const vector<string> &attached_option_values,
    bool uses_coordinator_storage_secret,
    bool distributed_replay_path_restricted,
    const string &data_storage_version);

void RegisterLanceDistributedWrites(ExtensionLoader &loader);

} // namespace duckdb

#endif
