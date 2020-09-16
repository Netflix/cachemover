#pragma once

#include "tasks/task.h"
#include "utils/status.h"

#include <iostream>
#include <string>

namespace memcachedumper {

class S3UploadFileTask : public Task {
 public:
  S3UploadFileTask(std::string fq_local_path, std::string filename);
  ~S3UploadFileTask();

  void Execute() override;
  Status GetUploadStatus() { return upload_status_; }

 private:
  std::string fq_local_path_;
  std::string filename_;

  Status upload_status_;

  Status SendSQSNotification(std::string s3_file_uri);
};

} // namespace memcachedumper
