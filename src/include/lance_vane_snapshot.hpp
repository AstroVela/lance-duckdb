#pragma once

#ifdef LANCE_VANE_DISTRIBUTED

#include "duckdb.hpp"

#include "lance_dataset_cache.hpp"

namespace duckdb {

shared_ptr<LanceDatasetCacheEntry>
LanceVaneGetOrOpenSnapshot(ClientContext &context, const string &path,
                           uint64_t version, const string &generation_id,
                           bool private_diagnostics);

} // namespace duckdb

#endif
