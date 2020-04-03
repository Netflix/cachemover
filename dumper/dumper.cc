#include "dumper/dumper.h"

#include "common/logger.h"
#include "tasks/metadump_task.h"
#include "tasks/task.h"
#include "tasks/task_scheduler.h"
#include "utils/mem_mgr.h"
#include "utils/socket_pool.h"

#include <iostream>
#include <sstream>
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

void DumperOptions::set_max_key_file_size(uint64_t max_key_file_size) {
  max_key_file_size_ = max_key_file_size;
}

void DumperOptions::set_max_data_file_size(uint64_t max_data_file_size) {
  max_data_file_size_ = max_data_file_size;
}

void DumperOptions::set_logfile_path(string_view logfile_path) {
  logfile_path_ = logfile_path;
}

Dumper::Dumper(DumperOptions& opts)
  : memcached_hostname_(opts.hostname()),
    memcached_port_(opts.port()),
    num_threads_(opts.num_threads()),
    max_key_file_size_(opts.max_key_file_size()),
    max_data_file_size_(opts.max_data_file_size()) {
  std::stringstream options_log;
  options_log << "Starting dumper with options: " << std::endl
            << "Hostname: " << opts.hostname() << std::endl
            << "Port: " << opts.port() << std::endl
            << "Num threads: " << opts.num_threads() << std::endl
            << "Chunk size: " << opts.chunk_size() << std::endl
            << "Max memory limit: " << opts.max_memory_limit() << std::endl
            << "Max key file size: " << opts.max_key_file_size() << std::endl
            << "Max data file size: " << opts.max_data_file_size() << std::endl
            << std::endl;
  LOG(options_log.str());

  socket_pool_.reset(new SocketPool(memcached_hostname_, memcached_port_, num_threads_));
  mem_mgr_.reset(new MemoryManager(
      opts.chunk_size(), opts.max_memory_limit() / opts.chunk_size()));
}

Dumper::~Dumper() = default;

Status Dumper::Init() {

  RETURN_ON_ERROR(socket_pool_->PrimeConnections());
  RETURN_ON_ERROR(mem_mgr_->PreallocateChunks());

  task_scheduler_.reset(new TaskScheduler(num_threads_, this));
  task_scheduler_->Init();

  return Status::OK();
}

Socket* Dumper::GetMemcachedSocket() {
  return socket_pool_->GetSocket();
}

void Dumper::ReleaseMemcachedSocket(Socket *sock) {
  return socket_pool_->ReleaseSocket(sock);
}

void Dumper::Run() {

  PrintTask *ptask = new PrintTask("Testing PrintTask!!", 77);
  task_scheduler_->SubmitTask(ptask);

  MetadumpTask *mtask = new MetadumpTask(0, "test_prefix", max_key_file_size_, mem_mgr_.get());
  task_scheduler_->SubmitTask(mtask);

  task_scheduler_->WaitUntilTasksComplete();
  LOG("Status: All tasks completed. Exiting...");
}

} // namespace memcachedumper
