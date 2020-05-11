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

DoneTask::DoneTask() {
}

void DoneTask::Execute() {
  std::ofstream done_file;
  done_file.open(MemcachedUtils::GetDataFinalPath() + "/DONE");
  done_file.write("DONE\n", 5);
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
