#include "dumper/dumper.h"

#include "tasks/metadump_task.h"
#include "tasks/task.h"
#include "tasks/task_scheduler.h"
#include "utils/mem_mgr.h"
#include "utils/socket_pool.h"

#include <iostream>
#include <string>

#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

using std::string;
using std::string_view;

namespace memcachedumper {

void DumperOptions::set_hostname(string_view hostname) {
  hostname_ = hostname;
}

void DumperOptions::set_port(int port) {
  port_ = port;
}

void DumperOptions::set_num_threads(int num_threads) {
  num_threads_ = num_threads;
}

void DumperOptions::set_chunk_size(uint64_t chunk_size) {
  chunk_size_ = chunk_size;
}

void DumperOptions::set_max_memory_limit(uint64_t max_memory_limit) {
  max_memory_limit_ = max_memory_limit;
}

Dumper::Dumper(DumperOptions& opts) {
  std::cout << "Starting dumper with options: " << std::endl
            << "Hostname: " << opts.hostname() << std::endl
            << "Port: " << opts.port() << std::endl
            << "Num threads: " << opts.num_threads() << std::endl
            << "Chunk size: " << opts.chunk_size() << std::endl
            << "Max memory limit: " << opts.max_memory_limit() << std::endl
            << "Max file size: " << opts.max_file_size() << std::endl
            << std::endl;
  memcached_hostname_ = opts.hostname();
  memcached_port_ = opts.port();
  num_threads_ = opts.num_threads();

  socket_pool_.reset(new SocketPool(memcached_hostname_, memcached_port_, num_threads_));
  mem_mgr_.reset(new MemoryManager(
      opts.chunk_size(), opts.max_memory_limit() / opts.chunk_size()));
}

Dumper::~Dumper() = default;

int Dumper::Init() {

  int ret = socket_pool_->PrimeConnections();
  if (ret < 0) return -1;

  if (mem_mgr_->PreallocateChunks() < 0) return -1;

  task_scheduler_.reset(new TaskScheduler(num_threads_, this));
  task_scheduler_->Init();

  return 0;
}

Socket* Dumper::GetMemcachedSocket() {
  return socket_pool_->GetSocket();
}

void Dumper::Run() {

  PrintTask *ptask = new PrintTask("Testing PrintTask!!", 77);
  task_scheduler_->SubmitTask(ptask);

  Socket *sock = GetMemcachedSocket();
  MetadumpTask *mtask = new MetadumpTask(sock, 0, "test_prefix", 1024 * 1024, mem_mgr_.get());
  task_scheduler_->SubmitTask(mtask);

  task_scheduler_->WaitUntilTasksComplete();
  std::cout << "Status: All Tasks completed. Exiting..." << std::endl;
}

} // namespace memcachedumper
