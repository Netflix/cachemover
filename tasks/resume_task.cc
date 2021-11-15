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

#include "common/logger.h"
#include "tasks/resume_task.h"

#include "utils/memcache_utils.h"
#include "tasks/process_metabuf_task.h"
#include "tasks/task_scheduler.h"
#include "tasks/task_thread.h"

#include <experimental/filesystem>
#include <fstream>
#include <iostream>
#include <string>

namespace fs = std::experimental::filesystem;
namespace memcachedumper {

ResumeTask::ResumeTask(bool is_s3_dump)
  : is_s3_dump_(is_s3_dump){
}

void ResumeTask::ProcessCheckpoints() {
  for (auto file : fs::directory_iterator(MemcachedUtils::GetKeyFilePath())) {
    std::string filename = file.path().filename();
    if (filename.rfind("CHECKPOINTS_", 0) == 0) {

      std::ifstream chkpt_file;
      chkpt_file.open(file.path());

      std::string key_filename;
      while (std::getline(chkpt_file, key_filename)) {
        if (unprocessed_files_.find(key_filename) != unprocessed_files_.end()) {
          LOG("Ignoring keyfile since it was already processed: {0}", key_filename);

          // Remove files seen in checkpoint files to leave only unprocessed keyfiles.
          unprocessed_files_.erase(key_filename);
        }
      }
      chkpt_file.close();
    }
  }
}

void ResumeTask::GetKeyFileList() {

  for (auto file : fs::directory_iterator(MemcachedUtils::GetKeyFilePath())) {
    std::string filename = file.path().filename();
    if (filename.rfind("key_", 0) == 0) {
      // Get all the file names into the set first.
      unprocessed_files_.insert(filename);
    }
  }
}

void ResumeTask::QueueUnprocessedFiles() {

  LOG("Queuing {0} files for processing.", unprocessed_files_.size());
  TaskScheduler* task_scheduler = owning_thread()->task_scheduler();
  for (auto& file : unprocessed_files_) {
    LOG("Queueing {0}.", file);
    ProcessMetabufTask *ptask = new ProcessMetabufTask(
        MemcachedUtils::GetKeyFilePath() + file, is_s3_dump_);
    task_scheduler->SubmitTask(ptask);
  }
}

void ResumeTask::Execute() {
  GetKeyFileList();
  ProcessCheckpoints();
  QueueUnprocessedFiles();
}

} // namespace memcachedumper
