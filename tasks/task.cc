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

DoneTask::DoneTask(uint64_t dumped, uint64_t skipped, uint64_t not_found,
    std::string time_taken_str)
  : dumped_(dumped),
    skipped_(skipped),
    not_found_(not_found),
    total_time_taken_str_(time_taken_str) {
}

std::string DoneTask::PrepareFinalMetricsString() {
  std::stringstream final_metrics;

  final_metrics <<
      "DONE" << std::endl <<
      "Total keys dumped: " << dumped_ << std::endl <<
      "Total keys skipped: " << skipped_ << std::endl <<
      "Total keys not found: " << not_found_ << std::endl <<
      "Total time taken: " << total_time_taken_str_ << std::endl;

  return final_metrics.str();
}

void DoneTask::Execute() {
  std::ofstream done_file(MemcachedUtils::GetDataFinalPath() + "/DONE");
  done_file << PrepareFinalMetricsString().c_str();
  done_file.close();
}


PrintTask::~PrintTask() = default;

PrintTask::PrintTask(std::string print_str, int num)
    : print_str_(print_str),
      num_(num) {
}

void PrintTask::Execute() {
  LOG("PrintTask: " + print_str_ + " || " + std::to_string(num_));
}


InfiniteTask::~InfiniteTask() = default;

InfiniteTask::InfiniteTask() {
}

void InfiniteTask::Execute() {
  while(1);
}


} // namespace memcachedumper
