#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

class LanceBlobFileSystem final : public FileSystem {
public:
  unique_ptr<FileHandle>
  OpenFile(const string &path, FileOpenFlags flags,
           optional_ptr<FileOpener> opener = nullptr) override;
  int64_t Read(FileHandle &handle, void *buffer, int64_t bytes) override;
  void Read(FileHandle &handle, void *buffer, int64_t bytes,
            idx_t location) override;
  int64_t GetFileSize(FileHandle &handle) override;
  timestamp_t GetLastModifiedTime(FileHandle &handle) override;
  string GetVersionTag(FileHandle &handle) override;
  FileType GetFileType(FileHandle &handle) override;
  void Seek(FileHandle &handle, idx_t location) override;
  idx_t SeekPosition(FileHandle &handle) override;
  bool CanSeek() override { return true; }
  bool OnDiskFile(FileHandle &handle) override { return false; }
  bool CanHandleFile(const string &path) override;
  string GetName() const override { return "LanceBlobFileSystem"; }
  vector<OpenFileInfo> Glob(const string &path,
                            FileOpener *opener = nullptr) override;
  bool FileExists(const string &path,
                  optional_ptr<FileOpener> opener = nullptr) override;
};

} // namespace duckdb
