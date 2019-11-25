#pragma once

#include "utils/status.h"

#include <memory>
#include <string>

namespace memcachedumper {

class MemoryManager;
class Socket;
class SocketPool;
class TaskScheduler;

// Options to configure the cache dumper.
class DumperOptions {
 public:

  void set_hostname(std::string_view hostname);
  void set_port(int port);
  void set_num_threads(int num_threads);
  void set_chunk_size(uint64_t chunk_size);
  void set_max_memory_limit(uint64_t max_memory_limit);
  void set_max_file_size(uint64_t max_file_size);
  void set_logfile_path(std::string_view logfile_path);

  std::string hostname() { return hostname_; }
  int port() { return port_; }
  int num_threads() { return num_threads_; }
  int chunk_size() { return chunk_size_; }
  int max_memory_limit() { return max_memory_limit_; }
  int max_file_size() { return max_file_size_; }
  std::string logfile_path() { return logfile_path_; }

 private:
  // Hostname that contains target memecached.
  std::string hostname_;
  // Memcached's port.
  int port_;
  // Number of worker threads for this cache dumper.
  int num_threads_;
  // The size to use for each allocated buffer (in bytes).
  uint64_t chunk_size_;
  // The maximum amount of memory to use (in bytes).
  uint64_t max_memory_limit_;
  // Max size per file we produce.
  uint64_t max_file_size_;
  // Path to the logfile.
  std::string logfile_path_;
};

class Dumper {
 public:
  Dumper(DumperOptions& opts);

  ~Dumper();

  // Initializes the dumper by connecting to memcached.
  Status Init();

  // Starts the dumping process.
  void Run();

  // Returns a socket to memcached from the socket pool.
  Socket* GetMemcachedSocket();

 private:
  std::string memcached_hostname_;
  int memcached_port_;
  int num_threads_;

  // Pool of sockets to talk to memcached.
  std::unique_ptr<SocketPool> socket_pool_;

  // Owned memory manager.
  std::unique_ptr<MemoryManager> mem_mgr_;

  // The task scheduler that will carry out all the work.
  std::unique_ptr<TaskScheduler> task_scheduler_;
};

} // namespace memcachedumper
