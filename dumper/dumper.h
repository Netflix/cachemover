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
  void set_max_key_file_size(uint64_t max_key_file_size);
  void set_max_data_file_size(uint64_t max_data_file_size);
  void set_log_file_path(std::string_view logfile_path);
  void set_output_dir_path(std::string_view output_dir_path);

  std::string hostname() { return hostname_; }
  int port() { return port_; }
  int num_threads() { return num_threads_; }
  uint64_t chunk_size() { return chunk_size_; }
  uint64_t max_memory_limit() { return max_memory_limit_; }
  uint64_t max_key_file_size() { return max_key_file_size_; }
  uint64_t max_data_file_size() { return max_data_file_size_; }
  std::string log_file_path() { return log_file_path_; }
  std::string output_dir_path() { return output_dir_path_; }

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
  // Max size per key file we produce.
  uint64_t max_key_file_size_;
  // Max size per data file we produce.
  uint64_t max_data_file_size_;
  // Path to the log file.
  std::string log_file_path_;
  // Path to the output directory.
  std::string output_dir_path_;
};

class Dumper {
 public:
  Dumper(DumperOptions& opts);

  ~Dumper();

  MemoryManager *mem_mgr() { return mem_mgr_.get(); }

  uint64_t max_key_file_size() { return max_key_file_size_; }
  uint64_t max_data_file_size() { return max_data_file_size_; }

  // Initializes the dumper by connecting to memcached.
  Status Init();

  // Starts the dumping process.
  void Run();

  // Returns a socket to memcached from the socket pool.
  Socket* GetMemcachedSocket();

  // Releases a memcached socket back to the socket pool.
  void ReleaseMemcachedSocket(Socket *sock);

 private:

  // Set up the output directories and make sure they're empty.
  Status CreateAndValidateOutputDirs();

  std::string memcached_hostname_;
  int memcached_port_;
  int num_threads_;
  uint64_t max_key_file_size_;
  uint64_t max_data_file_size_;
  std::string output_dir_path_;

  // Pool of sockets to talk to memcached.
  std::unique_ptr<SocketPool> socket_pool_;

  // Owned memory manager.
  std::unique_ptr<MemoryManager> mem_mgr_;

  // The task scheduler that will carry out all the work.
  std::unique_ptr<TaskScheduler> task_scheduler_;
};

} // namespace memcachedumper
