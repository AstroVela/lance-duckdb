#pragma once

#include "duckdb.hpp"
#include "lance_ffi.hpp"

namespace duckdb {

class LanceBlobAccess {
public:
  explicit LanceBlobAccess(ClientContext &context);
  ~LanceBlobAccess();
  LanceBlobAccess(const LanceBlobAccess &) = delete;
  LanceBlobAccess &operator=(const LanceBlobAccess &) = delete;
  void *Handle() const { return access; }

private:
  void *access = nullptr;
};

string LanceNormalizeBlobTarget(const string &uri, bool local_path = false);
void LanceCheckBlobAccess(DatabaseInstance &database, const string &target,
                          uint32_t operation);
// Consume the error immediately, before calling any other FFI function.
[[noreturn]] void LanceThrowBlobError(const char *operation);

} // namespace duckdb
