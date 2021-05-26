#pragma once

#include "utils/status.h"

#include <stdio.h>
#include <sys/uio.h>
#include <unistd.h>

#include <memory>
#include <string>

#include <openssl/md5.h>

namespace memcachedumper {

class FileUtils {
 public:
  static void MoveFile(std::string file_path, std::string dest_path);
  static Status CreateDirectory(std::string dir_path);
  static Status RemoveFile(std::string file_path);
  static Status RemoveDirectoryAndContents(std::string dir_path);
  static uint64_t GetSpaceAvailable(std::string path);
  static bool FileExists(std::string path);

  // This call opens the file and counts the number of lines.
  // Could be expensive for large files. Use only if necessary.
  static uint64_t CountNumLines(std::string path);
};

class PosixFile {
 public:
  PosixFile(std::string filename);

  const std::string filename() { return filename_; }
  int fd() { return fd_; }

  Status Open();

  Status WriteV(struct iovec* iovecs, int n_iovecs, ssize_t* nwritten);

  Status Close();

 private:
  const std::string filename_;
  int fd_;
  bool is_open_;
  bool is_closed_;
};


/// Wrapper around PosixFile that automatically rotates files when size threshold
/// is met.
class RotatingFile {
 public:
  RotatingFile(std::string file_path, std::string file_prefix,
      uint64_t max_file_size, bool suffix_checksum, bool s3_upload_on_close);

  RotatingFile(std::string file_path, std::string file_prefix,
      uint64_t max_file_size, std::string optional_dest_path,
      bool suffix_checksum, bool s3_upload_on_close);

  // Initialize by creating the first file.
  Status Init();

  Status WriteV(struct iovec* iovecs, int n_iovecs, ssize_t* nwritten);
  Status Fsync();
  Status FsyncDestDir();

  Status Finish();

 private:
  std::string file_path_;
  std::string file_prefix_;
  uint64_t max_file_size_;

  // Optional path to move the files to after closing them.
  std::string optional_dest_path_;

  // Calculate checksum and append to end of final filename if 'true'.
  bool suffix_checksum_;

  // If 'true', uploads every file created after Close().
  bool s3_upload_on_close_;

  // Used for calculating the checksum of the current file.
  // Used a unique pointer here so we can reset it for every new file after
  // every call to RotateFile().
  std::unique_ptr<MD5_CTX> md5_ctx_;

  // 'Epilogue' of current file, where we take all necessary actions before
  // before it is ready for closing.
  //
  // For now, we calculate the md5 hash (if suffix_checksum_ == true) and
  // move the file to the 'optional_dest_path_' if provided.
  //
  // Must be called AFTER all writes to file are done but BEFORE closing it.
  Status FinalizeCurrentFile();

  // Closes the current file and opens a new one.
  Status RotateFile();

  // The filname of the file before moving it to 'optional_dest_path_'.
  std::string staging_file_name_;
  // Current file handler.
  std::unique_ptr<PosixFile> cur_file_;
  // Number of files written to totally.
  int nfiles_;
  // Number of bytes written to the current file.
  uint64_t nwritten_current_;
  // Number of bytes written in total.
  uint64_t nwritten_total_;
};

} // namespace memcachedumper
