#include "utils/file_util.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>

namespace memcachedumper {

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

RotatingFile::RotatingFile(std::string file_prefix, uint64_t max_file_size)
  : file_prefix_(file_prefix),
    max_file_size_(max_file_size),
    cur_file_(nullptr),
    nfiles_(0),
    nwritten_current_(0),
    nwritten_total_(0) {
}

Status RotatingFile::Init() {
  cur_file_.reset(new PosixFile(
      std::string(file_prefix_ + "_" + std::to_string(nfiles_))));
  RETURN_ON_ERROR(cur_file_->Open());

  return Status::OK();
}

Status RotatingFile::RotateFile() {
  std::cout << "Rotating file " << file_prefix_ << std::endl;
  RETURN_ON_ERROR(cur_file_->Close());
  ++nfiles_;

  cur_file_.reset(new PosixFile(
      std::string(file_prefix_ + "_" + std::to_string(nfiles_))));
  RETURN_ON_ERROR(cur_file_->Open());
  return Status::OK();
}

Status RotatingFile::WriteV(struct iovec* iovecs, int n_iovecs, ssize_t* nwritten) {
  RETURN_ON_ERROR(cur_file_->WriteV(iovecs, n_iovecs, nwritten));
  assert(nwritten >= 0);

  nwritten_current_ += *nwritten;
  nwritten_total_ += *nwritten;

  if (nwritten_current_ > max_file_size_) {
    nwritten_current_ = 0;
    std::cout << "nwritten_current_: " << nwritten_current_ << " Max file size: " << max_file_size_ << std::endl;
    Status s = RotateFile();
    if (!s.ok()) {
      return Status::IOError("Error while rotating file", s.ToString());
    }
  }

  return Status::OK();
}

Status RotatingFile::Finish() {
  RETURN_ON_ERROR(cur_file_->Close());

  return Status::OK();
}

} // namespace memcachedumper
