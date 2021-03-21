#pragma once

#include "dumper/dumper.h"
#include "tasks/task_scheduler.h"

#include <string>
#include <thread>
#include <iostream>

#include <aws/core/Aws.h>

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

  int Init();

  void Join();

  inline void account_keys_processed(uint64_t num_keys) {
    num_keys_processed_ += num_keys;
  }
  inline void increment_keys_ignored() { num_keys_ignored_++; }
  inline void increment_keys_filtered() { num_keys_filtered_++; }
  inline void account_keys_missing(uint64_t num_keys) {
    num_keys_missing_ += num_keys;
  }

  uint64_t num_keys_processed() { return num_keys_processed_; }
  uint64_t num_keys_ignored() { return num_keys_ignored_; }
  uint64_t num_keys_missing() { return num_keys_missing_; }
  uint64_t num_keys_filtered() { return num_keys_filtered_; }

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
  uint64_t num_keys_filtered_;

};

} // namespace memcachedumper
