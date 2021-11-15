/**
 *
 *  Copyright 2018 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

#include "common/logger.h"
#include "tasks/s3_upload_task.h"
#include "utils/file_util.h"
#include "utils/memcache_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <experimental/filesystem>
#include <iostream>

namespace memcachedumper {

namespace fs = std::experimental::filesystem;

Status FileUtils::CreateDirectory(std::string dir_path) {
  if (fs::exists(dir_path)) {
    if (!fs::is_empty(dir_path)) {
      return Status::IOError(dir_path, "Given output path is not empty.");
    }
    return Status::OK();
  }

  LOG("Creating directory: {0}", dir_path);
  if (fs::create_directories(dir_path)) return Status::OK();

  return Status::IOError(dir_path, "Could not create directory.");
}

void FileUtils::MoveFile(std::string file_path, std::string dest_path) {
  fs::rename(file_path, dest_path);
}

Status FileUtils::RemoveFile(std::string file_path) {
  bool removed = fs::remove(file_path);
  if (!removed) return Status::IOError("RemoveFile(): File not found", file_path);;
  return Status::OK();
}

Status FileUtils::RemoveDirectoryAndContents(std::string dir_path) {
  std::error_code ec;
  std::uintmax_t n = fs::remove_all(dir_path, ec);
  return (n == static_cast<std::uintmax_t>(-1)) ?
    Status::IOError("Could not delete directory", ec.message()) :
    Status::OK();
}

uint64_t FileUtils::GetSpaceAvailable(std::string path) {
  fs::space_info si = fs::space(path.c_str());
  return si.free;
}

bool FileUtils::FileExists(std::string path) {
  return fs::exists(path) ? true : false;
}

uint64_t FileUtils::CountNumLines(std::string path) {
  std::ifstream ifile(path);
  // No need to explicitly close() because RAII
  return std::count(std::istreambuf_iterator<char>(ifile),
                    std::istreambuf_iterator<char>(), '\n');
}

PosixFile::PosixFile(std::string filename)
  : filename_(filename) {
}

Status PosixFile::Open() {
  int fd = open(filename_.c_str(), O_CREAT | O_RDWR, 0666);
  if (fd < 0) {
    return Status::IOError(filename_, strerror(errno));
  }

  is_open_ = true;
  fd_ = fd;
  return Status::OK();
}

Status PosixFile::WriteV(struct iovec* iovecs, int n_iovecs, ssize_t* nwritten) {
  *nwritten = writev(fd_, iovecs, n_iovecs);

  if (*nwritten < 0) {
    return Status::IOError(filename_, strerror(errno));
  }

  return Status::OK();
}

Status PosixFile::Close() {
  if (!is_open_) return Status::IOError("File is not open. Cannot close ", filename_);

  int ret = close(fd_);
  if (ret < 0) {
    return Status::IOError(filename_, strerror(errno));
  }

  return Status::OK();
}

RotatingFile::RotatingFile(std::string file_path, std::string file_prefix,
    uint64_t max_file_size, bool suffix_checksum, bool s3_upload_on_close)
  : file_path_(file_path),
    file_prefix_(file_prefix),
    max_file_size_(max_file_size),
    optional_dest_path_(""),
    suffix_checksum_(suffix_checksum),
    s3_upload_on_close_(s3_upload_on_close),
    cur_file_(nullptr),
    nfiles_(0),
    nwritten_current_(0),
    nwritten_total_(0) {
}

RotatingFile::RotatingFile(std::string file_path, std::string file_prefix,
    uint64_t max_file_size, std::string optional_dest_path,
    bool suffix_checksum, bool s3_upload_on_close)
  : file_path_(file_path),
    file_prefix_(file_prefix),
    max_file_size_(max_file_size),
    optional_dest_path_(optional_dest_path),
    suffix_checksum_(suffix_checksum),
    s3_upload_on_close_(s3_upload_on_close),
    cur_file_(nullptr),
    nfiles_(0),
    nwritten_current_(0),
    nwritten_total_(0) {
}

Status RotatingFile::Init() {

  staging_file_name_ = file_prefix_ + "_" + std::to_string(nfiles_);
  cur_file_.reset(new PosixFile(
      std::string(file_path_ + staging_file_name_)));
  RETURN_ON_ERROR(cur_file_->Open());

  if (suffix_checksum_) {
    md5_ctx_.reset(new MD5_CTX());
    int ret = MD5_Init(md5_ctx_.get());
    if (ret != 1) return Status::IOError("MD5_Init failed");
  }

  return Status::OK();
}

Status RotatingFile::Fsync() {
  int ret = fsync(cur_file_->fd());
  if (ret < 0) {
    int err = errno;
    return Status::IOError("Could not fsync() file " + staging_file_name_, strerror(err));
  }

  return Status::OK();
}


Status RotatingFile::FsyncDestDir() {
  std::string dest_dir = MemcachedUtils::GetDataFinalPath();
  int dir_fd = open(dest_dir.c_str(), O_RDONLY);
  if (dir_fd < 0) {
    return Status::IOError(dest_dir, strerror(errno));
  }

  if (fsync(dir_fd) < 0) {
    return Status::IOError("Could not fsync() directory " + dest_dir, strerror(errno));
  }

  if (close(dir_fd) < 0) {
    return Status::IOError(dest_dir, strerror(errno));
  }

  return Status::OK();
}


Status RotatingFile::FinalizeCurrentFile() {
  std::string final_filename_only;
  std::string final_filename_fq;

  // If requested, append the file checksum to the filename.
  if (suffix_checksum_) {
    unsigned char digest[MD5_DIGEST_LENGTH];
    int ret = MD5_Final(digest, md5_ctx_.get());
    if (ret != 1) return Status::IOError("MD5_Final failed");

    std::string digest_hex;
    digest_hex.reserve(MD5_DIGEST_LENGTH);

    for (std::size_t i = 0; i != 16; ++i) {
      digest_hex += "0123456789ABCDEF"[digest[i] / 16];
      digest_hex += "0123456789ABCDEF"[digest[i] % 16];
    }

    md5_ctx_.reset(new MD5_CTX());
    ret = MD5_Init(md5_ctx_.get());
    if (ret != 1) return Status::IOError("MD5_Init failed");

    final_filename_only = file_prefix_ + "_" + digest_hex;
    final_filename_fq = optional_dest_path_ + final_filename_only;
  } else {

    final_filename_only = "_" + staging_file_name_;
    // Use the staging file name if a checksum wasn't requested.
    final_filename_fq = optional_dest_path_ + final_filename_only;
  }

  // Explicitly fsync()
  RETURN_ON_ERROR(Fsync());
  RETURN_ON_ERROR(cur_file_->Close());

  // If requested, move the file to the final path.
  if (!optional_dest_path_.empty()) {
    FileUtils::MoveFile(cur_file_->filename(), final_filename_fq);
    LOG("File: {0} complete.", final_filename_fq);
    RETURN_ON_ERROR(FsyncDestDir());
  }

  // TODO: This isn't where we would do the upload ideally.
  // Find a better way to do it.
  if (s3_upload_on_close_) {

    // Upload file to S3 and send a SQS notification.
    S3UploadFileTask s3_task(final_filename_fq, final_filename_only);
    s3_task.Execute();

    // If the file upload was a failure, return the error.
    // TODO: Add a retry loop if necessary.
    RETURN_ON_ERROR(s3_task.GetUploadStatus());

    // Since we've uploaded it to S3, delete it from the local FS.
    RETURN_ON_ERROR(FileUtils::RemoveFile(final_filename_fq));
  }

  return Status::OK();
}

Status RotatingFile::RotateFile() {

  RETURN_ON_ERROR(FinalizeCurrentFile());
  ++nfiles_;

  staging_file_name_ = file_prefix_ + "_" + std::to_string(nfiles_);
  cur_file_.reset(new PosixFile(
      std::string(file_path_ + staging_file_name_)));
  RETURN_ON_ERROR(cur_file_->Open());
  return Status::OK();
}

Status RotatingFile::WriteV(struct iovec* iovecs, int n_iovecs, ssize_t* nwritten) {

  if (suffix_checksum_) {
    for (int i = 0; i < n_iovecs; ++i) {

      int ret = MD5_Update(md5_ctx_.get(), iovecs[i].iov_base, iovecs[i].iov_len);
      if (ret != 1) return Status::IOError("MD5_Update failed");
    }
  }

  RETURN_ON_ERROR(cur_file_->WriteV(iovecs, n_iovecs, nwritten));
  assert(nwritten >= 0);

  nwritten_current_ += *nwritten;
  nwritten_total_ += *nwritten;

  if (nwritten_current_ > max_file_size_) {
    nwritten_current_ = 0;
    Status s = RotateFile();
    if (!s.ok()) {
      return Status::IOError("Error while rotating file", s.ToString());
    }
  }

  return Status::OK();
}

Status RotatingFile::Finish() {

  RETURN_ON_ERROR(FinalizeCurrentFile());
  return Status::OK();
}

} // namespace memcachedumper
