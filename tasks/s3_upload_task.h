/**
 *
 *  Copyright 2021 Netflix, Inc.
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

  Status SendSQSNotification(std::string s3_file_uri, std::string sqs_msg_body);
};

} // namespace memcachedumper
