#pragma once

#include "utils/status.h"

#include <string>

namespace memcachedumper {

class PosixFile {
 public:
 PosixFile(std::string filename);

  Status Open();

  Status Close();

 private:
  const std::string filename_;
  int fd_;
  bool is_open_;
  bool is_closed_;
};

} // namespace memcachedumper
