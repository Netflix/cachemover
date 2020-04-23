#pragma once

#include "tasks/task.h"
#include "utils/status.h"

#include <string>

namespace memcachedumper {

class MemoryManager;
class Socket;

class MetadumpTask : public Task {
 public:
  MetadumpTask(int slab_class, const std::string& file_prefix,
      uint64_t max_file_size, MemoryManager *mem_mgr);
  ~MetadumpTask() = default;

  void Execute() override;

 private:
  Status SendCommand(const std::string& metadump_cmd);
  Status RecvResponse();

  Socket *memcached_socket_;

  // The slab class to dump.
  // '0' indicates "all".
  int slab_class_;

  // Prefix to use for files while dumping data.
  std::string file_prefix_;

  // Size at which to start writing to a new file.
  uint64_t max_file_size_;

  // Pointer to a memory manager.
  MemoryManager *mem_mgr_;

};

} // namespace memcachedumper
