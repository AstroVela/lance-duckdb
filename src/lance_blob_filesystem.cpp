#include "lance_blob_filesystem.hpp"

#include "lance_blob.hpp"
#include "lance_blob_access.hpp"
#include "lance_common.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"

#include <cstring>
#include <limits>

namespace duckdb {

static constexpr idx_t BLOB_READ_AHEAD = 1024 * 1024;

struct BlobOwner {
  ~BlobOwner() { lance_blob_free(handle); }
  void *handle = nullptr;
};

class LanceBlobFileHandle final : public FileHandle {
public:
  LanceBlobFileHandle(FileSystem &filesystem, const string &path,
                      FileOpenFlags flags, unique_ptr<BlobOwner> blob_p,
                      uint64_t size_p, shared_ptr<DatabaseInstance> database_p)
      : FileHandle(filesystem, path, flags), database(std::move(database_p)),
        blob(std::move(blob_p)), size(size_p) {}
  // Member RAII provides a non-throwing destructor fallback. Explicit Close
  // serializes with operations; destruction still requires exclusive ownership.
  ~LanceBlobFileHandle() override = default;

  void Close() override {
    lock_guard<mutex> guard(lock);
    blob.reset();
    vector<uint8_t>().swap(window);
    window_size = 0;
  }

  void RequireOpen() const {
    if (!blob) {
      throw IOException("Lance Blob handle is closed");
    }
  }

  void ReadExact(void *buffer, idx_t count, uint64_t offset) {
    if (count == 0) {
      return;
    }
    if (!buffer) {
      throw InvalidInputException("Lance Blob read requires a buffer");
    }
    try {
      if (offset >= window_start && offset - window_start <= window_size &&
          count <= window_size - (offset - window_start)) {
        std::memcpy(buffer, window.data() + (offset - window_start), count);
      } else if (count >= BLOB_READ_AHEAD) {
        window_size = 0;
        ReadRange(static_cast<uint8_t *>(buffer), count, offset);
      } else {
        window_size = 0;
        auto length = MinValue<uint64_t>(BLOB_READ_AHEAD, size - offset);
        window.resize(NumericCast<idx_t>(length));
        ReadRange(window.data(), window.size(), offset);
        window_start = offset;
        window_size = length;
        std::memcpy(buffer, window.data(), count);
      }
    } catch (...) {
      window_size = 0;
      throw;
    }
  }

  // Even a closed FileHandle dispatches through its database-owned filesystem.
  // Retain that subsystem until the C++ handle itself is destroyed.
  shared_ptr<DatabaseInstance> database;
  mutex lock;
  unique_ptr<BlobOwner> blob;
  const uint64_t size;
  uint64_t cursor = 0;
  vector<uint8_t> window;
  uint64_t window_start = 0;
  uint64_t window_size = 0;

private:
  void ReadRange(uint8_t *buffer, idx_t count, uint64_t offset) {
    size_t read = 0;
    if (lance_blob_read_at(blob->handle, offset, buffer,
                           NumericCast<size_t>(count), &read) != 0) {
      LanceThrowBlobError("read");
    }
    if (read != count) {
      throw IOException("Lance Blob read returned an incomplete range");
    }
  }
};

static ClientContext &RequireContext(optional_ptr<FileOpener> opener) {
  auto context = FileOpener::TryGetClientContext(opener);
  if (!context) {
    throw InvalidInputException(
        "Opening a Lance Blob requires a DuckDB client context");
  }
  return *context;
}

bool LanceBlobFileSystem::CanHandleFile(const string &path) {
  return StringUtil::StartsWith(path, "lance-blob://");
}

unique_ptr<FileHandle>
LanceBlobFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                              optional_ptr<FileOpener> opener) {
  if (!flags.OpenForReading() || flags.OpenForWriting() ||
      flags.OpenForAppending() || flags.CreateFileIfNotExists() ||
      flags.OverwriteExistingFile() || flags.CreatePrivateFile() ||
      flags.ExclusiveCreate() || flags.ReturnNullIfExists() ||
      flags.DirectIO() || flags.Lock() != FileLockType::NO_LOCK) {
    throw InvalidInputException(
        "Lance Blob files support read-only opens without creation or locking");
  }
  auto uri = ParseLanceBlobUri(path);
  auto &context = RequireContext(opener);
  LanceCheckBlobAccess(DatabaseInstance::GetDatabase(context), path,
                       LANCE_BLOB_READ);
  LanceBlobAccess access(context);
  auto blob = make_uniq<BlobOwner>();
  uint64_t size = 0;
  LanceBlobOpenOptions options{uri.dataset.c_str(),  uri.version,
                               uri.snapshot.c_str(), uri.column.c_str(),
                               uri.row_id,           access.Handle()};
  if (lance_blob_open(&options, &blob->handle, &size) != 0) {
    if (lance_last_error_code() == LANCE_BLOB_ERROR_NOT_FOUND &&
        flags.ReturnNullIfNotExists()) {
      LanceConsumeLastError();
      return nullptr;
    }
    LanceThrowBlobError("open");
  }
  if (size > uint64_t(std::numeric_limits<int64_t>::max())) {
    throw IOException("Lance Blob is too large for DuckDB's file interface");
  }
  return make_uniq<LanceBlobFileHandle>(
      *this, path, flags, std::move(blob), size,
      DatabaseInstance::GetDatabase(context).shared_from_this());
}

int64_t LanceBlobFileSystem::Read(FileHandle &handle, void *buffer,
                                  int64_t bytes) {
  auto &file = handle.Cast<LanceBlobFileHandle>();
  lock_guard<mutex> guard(file.lock);
  file.RequireOpen();
  if (bytes < 0) {
    throw InvalidInputException("Lance Blob read length must be nonnegative");
  }
  auto count = NumericCast<idx_t>(
      MinValue<uint64_t>(uint64_t(bytes), file.size - file.cursor));
  file.ReadExact(buffer, count, file.cursor);
  file.cursor += count;
  return NumericCast<int64_t>(count);
}

void LanceBlobFileSystem::Read(FileHandle &handle, void *buffer, int64_t bytes,
                               idx_t location) {
  auto &file = handle.Cast<LanceBlobFileHandle>();
  lock_guard<mutex> guard(file.lock);
  file.RequireOpen();
  if (bytes < 0 || location > file.size ||
      uint64_t(bytes) > file.size - location) {
    throw InvalidInputException("Lance Blob positional read exceeds its size");
  }
  file.ReadExact(buffer, NumericCast<idx_t>(bytes), location);
}

void LanceBlobFileSystem::Seek(FileHandle &handle, idx_t location) {
  auto &file = handle.Cast<LanceBlobFileHandle>();
  lock_guard<mutex> guard(file.lock);
  file.RequireOpen();
  if (location > file.size) {
    throw InvalidInputException("Lance Blob seek exceeds its size");
  }
  file.cursor = location;
  if (location < file.window_start ||
      location - file.window_start >= file.window_size) {
    file.window_size = 0;
  }
}

idx_t LanceBlobFileSystem::SeekPosition(FileHandle &handle) {
  auto &file = handle.Cast<LanceBlobFileHandle>();
  lock_guard<mutex> guard(file.lock);
  file.RequireOpen();
  return NumericCast<idx_t>(file.cursor);
}

int64_t LanceBlobFileSystem::GetFileSize(FileHandle &handle) {
  auto &file = handle.Cast<LanceBlobFileHandle>();
  lock_guard<mutex> guard(file.lock);
  file.RequireOpen();
  return NumericCast<int64_t>(file.size);
}

timestamp_t LanceBlobFileSystem::GetLastModifiedTime(FileHandle &handle) {
  GetFileSize(handle);
  return timestamp_t(0);
}

string LanceBlobFileSystem::GetVersionTag(FileHandle &handle) {
  GetFileSize(handle);
  return "";
}

FileType LanceBlobFileSystem::GetFileType(FileHandle &handle) {
  GetFileSize(handle);
  return FileType::FILE_TYPE_REGULAR;
}

vector<OpenFileInfo> LanceBlobFileSystem::Glob(const string &path,
                                               FileOpener *opener) {
  ParseLanceBlobUri(path);
  auto &context = RequireContext(opener);
  LanceCheckBlobAccess(DatabaseInstance::GetDatabase(context), path,
                       LANCE_BLOB_READ);
  return {OpenFileInfo(path)};
}

bool LanceBlobFileSystem::FileExists(const string &path,
                                     optional_ptr<FileOpener> opener) {
  auto handle = OpenFile(path,
                         FileFlags::FILE_FLAGS_READ |
                             FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS,
                         opener);
  return bool(handle);
}

} // namespace duckdb
