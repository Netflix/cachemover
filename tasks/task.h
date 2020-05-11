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


/* -- For debugging -- */
class PrintTask : public Task {
 public:
  PrintTask(std::string print_str, int num);
  ~PrintTask();

  void Execute() override;

 private:
  std::string print_str_;
  int num_;
};

class InfiniteTask : public Task {
 public:
  InfiniteTask();
  ~InfiniteTask();

  void Execute() override;
};

} // namespace memcachedumper
