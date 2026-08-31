#pragma once

#ifdef LANCE_VANE_DISTRIBUTED

#include "lance_vane_search.hpp"

namespace duckdb {

class LanceTableEntry;
class LanceDatasetCacheEntry;

// Resolve a REST namespace table through the standard DescribeTable contract
// and freeze the independently opened physical Lance snapshot. Failures are
// recorded on the candidate and are intentionally deferred until distributed
// admission so the existing local namespace execution path remains unchanged.
void LanceVaneResolveRestPhysicalCandidate(
    ClientContext &context, const LanceTableEntry &table,
    const shared_ptr<LanceDatasetCacheEntry> &bound_dataset_entry,
    LanceVanePhysicalCandidate &out_candidate);

} // namespace duckdb

#endif
