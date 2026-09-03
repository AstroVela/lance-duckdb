#pragma once

#include "duckdb.hpp"

namespace duckdb {

class Catalog;
class ClientContext;
class LanceTableEntry;

// Return a clone of the transaction-local Lance dataset for this table, or
// nullptr when the current DuckDB transaction has not staged any Lance changes.
// The caller owns the returned dataset handle.
void *LanceTryOpenTransactionDataset(ClientContext &context,
                                     const LanceTableEntry &table);

// Return the opaque transaction-local workspace for this table, creating it
// from dataset when necessary. The transaction manager owns the returned
// workspace; dataset remains owned by the caller.
void *LanceGetOrCreateDatasetTransaction(ClientContext &context,
                                         const LanceTableEntry &table,
                                         void *dataset);

} // namespace duckdb
