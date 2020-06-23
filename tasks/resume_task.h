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
