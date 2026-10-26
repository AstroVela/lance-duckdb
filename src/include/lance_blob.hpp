#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct LanceBlobUri {
  string dataset;
  uint64_t version = 0;
  string snapshot;
  string column;
  uint64_t row_id = 0;
};

string EncodeLanceBlobUri(const LanceBlobUri &uri);
LanceBlobUri ParseLanceBlobUri(const string &path);
void RegisterLanceBlobFunctions(ExtensionLoader &loader);

} // namespace duckdb
