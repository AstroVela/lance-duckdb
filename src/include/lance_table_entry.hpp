#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

struct AlterInfo;
struct CatalogTransaction;
class CatalogEntry;
class ClientContext;

enum class LanceNamespaceKind { Directory, Rest };

struct LanceNamespaceTableConfig {
  LanceNamespaceKind kind = LanceNamespaceKind::Rest;

  string root;
  vector<string> option_keys;
  vector<string> option_values;
#ifdef LANCE_VANE_DISTRIBUTED
  // TYPE LANCE secrets remain coordinator-local in Vane distributed scans.
  // Preserve only this provenance bit across catalog-entry copies so planning
  // can reject the scan; secret values are never serialized to workers.
  bool uses_coordinator_storage_secret = false;
  // Preserve whether the original ATTACH path contained private URI
  // components even if namespace resolution normalized them into a local path.
  bool distributed_replay_path_restricted = false;
  // REST credentials resolved on the coordinator (including named secrets)
  // must taint diagnostics even when the endpoint itself is public.
  bool uses_coordinator_auth_secret = false;
#endif

  string endpoint;
  string table_id;
  string delimiter;
  string bearer_token_override;
  string api_key_override;
  string headers_tsv;
  string display_uri;
#ifdef LANCE_VANE_DISTRIBUTED
  // Bind-local exact version for namespace query-table execution. Catalog
  // entries leave this unset so ordinary non-distributed reads remain fresh.
  uint64_t snapshot_version = 0;
#endif

  bool IsDirectory() const { return kind == LanceNamespaceKind::Directory; }
  bool IsRest() const { return kind == LanceNamespaceKind::Rest; }
};

// LanceTableEntry represents a Lance dataset as a DuckDB base table entry.
// It supports scanning via a Lance-backed table scan function and appending via
// DuckDB's INSERT planning path (implemented at the catalog level).
class LanceTableEntry final : public TableCatalogEntry {
public:
  LanceTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                  CreateTableInfo &info, string dataset_uri);
  LanceTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                  CreateTableInfo &info, LanceNamespaceTableConfig config);

  unique_ptr<CatalogEntry> AlterEntry(ClientContext &context,
                                      AlterInfo &info) override;
  unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction transaction,
                                      AlterInfo &info) override;

  unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

  TableFunction GetScanFunction(ClientContext &context,
                                unique_ptr<FunctionData> &bind_data) override;

  unique_ptr<BaseStatistics> GetStatistics(ClientContext &, column_t) override {
    return nullptr;
  }

  TableStorageInfo GetStorageInfo(ClientContext &) override { return {}; }

  const string &DatasetUri() const { return dataset_uri; }
  bool IsNamespaceBacked() const { return namespace_config != nullptr; }
  const LanceNamespaceTableConfig &NamespaceConfig() const {
    if (!namespace_config) {
      throw InternalException("LanceTableEntry is not namespace-backed");
    }
    return *namespace_config;
  }

  // Top-level catalog columns whose declared type was coerced to a
  // DuckDB-compatible shape by the Arrow-compat reader-boundary layer
  // (e.g. FloatingPoint(HALF) → FloatingPoint(SINGLE)). Writers must refuse
  // to operate on such columns — DuckDB would hand back values in the
  // coerced type and silently widen / otherwise corrupt the on-disk storage.
  bool HasCoercedColumns() const { return !coerced_column_names.empty(); }
  const vector<string> &CoercedColumnNames() const {
    return coerced_column_names;
  }
  void SetCoercedColumnNames(vector<string> names) {
    coerced_column_names = std::move(names);
  }

private:
  string dataset_uri;
  unique_ptr<LanceNamespaceTableConfig> namespace_config;
  vector<string> coerced_column_names;
};

} // namespace duckdb
