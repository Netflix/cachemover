#pragma once

#include "tasks/task.h"

#include <string>

namespace memcachedumper {

class MemoryManager;
class Socket;

class MetadumpTask : public Task {
 public:
  MetadumpTask(Socket *socket, int slab_class, const std::string& file_prefix,
      uint64_t max_file_size, MemoryManager *mem_mgr);
  void Execute() override;

 private:
  int SendCommand(const std::string& metadump_cmd);
  int RecvResponse();

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
