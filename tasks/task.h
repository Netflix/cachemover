#pragma once

#include <iostream>
#include <string>

namespace memcachedumper {

class MemoryManager;
class TaskThread;

class Task {
public:
  virtual ~Task() = default;
  //virtual ~Task();
  TaskThread* owning_thread() { return owning_thread_; }

  void set_owning_thread(TaskThread* thread) { owning_thread_ = thread; }

  virtual void Execute() = 0;
  //std::atomic<bool> running_;

 private:
  TaskThread *owning_thread_;

};

class DoneTask : public Task {
 public:
  DoneTask(uint64_t dumped, uint64_t skipped, uint64_t not_found,
      std::string time_taken_str);
  ~DoneTask();

  void Execute() override;

 private:
  uint64_t dumped_;
  uint64_t skipped_;
  uint64_t not_found_;
  std::string total_time_taken_str_;

  std::string PrepareFinalMetricsString();
};

class InfiniteTask : public Task {
 public:
  InfiniteTask();
  ~InfiniteTask();

  void Execute() override;
};

} // namespace memcachedumper
