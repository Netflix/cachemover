#pragma once

#include "utils/status.h"

#include <stdio.h>
#include <sys/uio.h>
#include <unistd.h>

#include <memory>
#include <string>

namespace memcachedumper {

class FileUtils {
 public:
  static void MoveFile(std::string file_path, std::string dest_path);
  static Status CreateDirectory(std::string dir_path);
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
  RotatingFile(std::string file_path, std::string file_prefix, uint64_t max_file_size);
  RotatingFile(std::string file_path, std::string file_prefix, uint64_t max_file_size,
    std::string optional_dest_path);

  // Initialize by creating the first file.
  Status Init();

  Status WriteV(struct iovec* iovecs, int n_iovecs, ssize_t* nwritten);
  Status Fsync();

  Status Finish();

 private:
  std::string file_path_;
  std::string file_prefix_;
  uint64_t max_file_size_;

  // Optional path to move the files to after closing them.
  std::string optional_dest_path_;

  Status RotateFile();

  // Current filename.
  std::string cur_file_name_;
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
