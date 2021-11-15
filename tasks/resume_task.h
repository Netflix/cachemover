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

#include <set>
#include <string>

namespace memcachedumper {

class ResumeTask : public Task {
 public:
  ResumeTask(bool is_s3_dump);
  ~ResumeTask() = default;

  void Execute() override;

  void GetKeyFileList();

  void ProcessCheckpoints();

  void QueueUnprocessedFiles();

 private:
  std::set<std::string> unprocessed_files_;

  // Passes on this value to a ProcessMetabufTask.
  bool is_s3_dump_;
};

} // namespace memcachedumper
