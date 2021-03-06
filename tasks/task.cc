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

#include "tasks/task.h"

#include "common/logger.h"
#include "tasks/task_scheduler.h"
#include "tasks/task_thread.h"
#include "utils/mem_mgr.h"
#include "utils/memcache_utils.h"
#include "utils/socket.h"

#include <unistd.h>
#include <string.h>

#include <fstream>
#include <sstream>

namespace memcachedumper {

DoneTask::~DoneTask() = default;

DoneTask::DoneTask(uint64_t total,
    uint64_t dumped,
    uint64_t skipped,
    uint64_t not_found,
    uint64_t filtered,
    std::string time_taken_str) : total_(total),
                                  dumped_(dumped),
                                  skipped_(skipped),
                                  not_found_(not_found),
                                  filtered_(filtered),
                                  total_time_taken_str_(time_taken_str) {
}

std::string DoneTask::PrepareFinalMetricsString() {
  std::stringstream final_metrics;

  final_metrics <<
      "DONE" << std::endl <<
      "Total keys from metadump: " << total_ << std::endl <<
      "Total keys dumped: " << dumped_ << std::endl <<
      "Total keys skipped: " << skipped_ << std::endl <<
      "Total keys not found: " << not_found_ << std::endl <<
      "Total keys filtered: " << filtered_ << std::endl <<
      "Total time taken: " << total_time_taken_str_ << std::endl;

  return final_metrics.str();
}

void DoneTask::Execute() {
  std::ofstream done_file(MemcachedUtils::GetDataFinalPath() + "/DONE");
  done_file << PrepareFinalMetricsString().c_str();
  done_file.close();
}

InfiniteTask::~InfiniteTask() = default;

InfiniteTask::InfiniteTask() {
}

void InfiniteTask::Execute() {
  while(1);
}


} // namespace memcachedumper
