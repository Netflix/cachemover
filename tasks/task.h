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

class PrintTask : public Task {
 public:
  PrintTask(std::string print_str, int num);
  ~PrintTask();

  void Execute() override;

 private:
  std::string print_str_;
  int num_;
};

class PrintKeysFromFileTask : public Task {
 public:
  PrintKeysFromFileTask(const std::string& filename);
  ~PrintKeysFromFileTask() = default;
  void Execute() override;

 private:
  std::string filename_;
};

} // namespace memcachedumper
