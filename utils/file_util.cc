#include "utils/file_util.h"

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <errno.h>

namespace memcachedumper {

PosixFile::PosixFile(std::string filename)
  : filename_(filename) {
}

Status PosixFile::Open() {
  int fd = open(filename_.c_str(), O_CREAT, 0666);
  if (fd < 0) {
    return Status::IOError(filename_, strerror(errno));
  }

  is_open_ = true;
  fd_ = fd;
  return Status::OK();
}

/*Status PosixFile::WriteV(struct iovec *iovecs, int32_t *nwritten) {
  
}*/

Status PosixFile::Close() {
  if (!is_open_) return Status::IOError("File is not open. Cannot close ", filename_);

  int ret = close(fd_);
  if (ret < 0) {
    return Status::IOError(filename_, strerror(errno));
  }

  return Status::OK();
}

} // namespace memcachedumper
