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

PrintTask::~PrintTask() = default;

PrintTask::PrintTask(std::string print_str, int num)
    : print_str_(print_str),
      num_(num) {
}

void PrintTask::Execute() {
  LOG("PrintTask: " + print_str_ + " || " + std::to_string(num_));
}

} // namespace memcachedumper
