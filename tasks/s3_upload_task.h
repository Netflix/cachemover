#pragma once

#include "tasks/task.h"

#include <iostream>
#include <string>

namespace memcachedumper {

class S3UploadTask : public Task {
 public:
  S3UploadTask(std::string bucket_name, std::string path, std::string file_prefix);
  ~S3UploadTask();

  void Execute() override;

 private:
  std::string bucket_name_;
  std::string path_;
  std::string file_prefix_;
};
} // namespace memcachedumper
