#pragma once

#include "dumper/dumper.h"
#include "tasks/task_scheduler.h"

#include <string>
#include <thread>
#include <iostream>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

namespace memcachedumper {

class MemoryManager;
class Socket;
class Task;
class TaskScheduler;

class TaskThread {
 public:
  TaskThread(TaskScheduler *task_scheduler_, const std::string& thread_name);
  ~TaskThread();

  TaskScheduler *task_scheduler() { return task_scheduler_; }

  std::string thread_name() { return thread_name_; }

  MemoryManager *mem_mgr();

  Aws::S3::S3Client* s3_client() { return task_scheduler_->dumper()->GetS3Client(); }

  int Init();

  void Join();

  void account_keys_processed(uint64_t num_keys) { num_keys_processed_ += num_keys; }
  void increment_keys_ignored() { num_keys_ignored_++; }
  void account_keys_missing(uint64_t num_keys) { num_keys_missing_ += num_keys; }

  uint64_t num_keys_processed() { return num_keys_processed_; }
  uint64_t num_keys_ignored() { return num_keys_ignored_; }
  uint64_t num_keys_missing() { return num_keys_missing_; }

  void PrintNumKeysProcessed() {
    std::cout << "Thread (" << thread_name_.c_str() << ") - Num keys: " << num_keys_processed_ << std::endl;
  }
 private:

  void WorkerLoop();

  Task* WaitForNextTask();

  bool ShuttingDown();

  // Back pointer to owning TaskScheduler.
  TaskScheduler *task_scheduler_;

  // Name of thread.
  std::string thread_name_;

  std::thread thread_;

  uint64_t num_keys_processed_;
  uint64_t num_keys_ignored_;
  uint64_t num_keys_missing_;

};

} // namespace memcachedumper
