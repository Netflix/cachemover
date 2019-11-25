#include "tasks/task.h"

#include "common/logger.h"
#include "utils/mem_mgr.h"

#include <string.h>

#include <fstream>

namespace memcachedumper {

PrintTask::~PrintTask() = default;

PrintTask::PrintTask(std::string print_str, int num)
    : print_str_(print_str),
      num_(num) {
}

void PrintTask::Execute() {
  LOG("PrintTask: " + print_str_ + " || " + std::to_string(num_));
}

PrintKeysFromFileTask::PrintKeysFromFileTask(const std::string& filename,
    MemoryManager *mem_mgr)
  : filename_(filename),
    mem_mgr_(mem_mgr) {
}

void PrintKeysFromFileTask::Execute() {
  std::ifstream file;
  file.open(filename_);

  LOG("Starting PrintKeysFromFileTask");

  std::string key, exp, la, cas, fetch, cls, size;
  std::ofstream keyfile;
  keyfile.open("KEYFILE_" + filename_ + ".txt");

  // Note: This is just a best effort printing of keys.
  // We should ideally use the memory manager and use its buffers, but this is
  // just a test task, so we don't care for now.
  while (file >> key >> exp >> la >> cas >> fetch >> cls >> size) {
    keyfile << key << std::endl;
  }
  file.close();
  keyfile.close();
}

} // namespace memcachedumper
