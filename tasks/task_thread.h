#pragma once

#include <string>
#include <thread>

namespace memcachedumper {

class Socket;
class Task;
class TaskScheduler;

class TaskThread {
 public:
  TaskThread(TaskScheduler *task_scheduler_, const std::string& thread_name);
  ~TaskThread();

  TaskScheduler *task_scheduler() { return task_scheduler_; }

  int Init();

  void Join();

 private:

  void WorkerLoop();

  Task* WaitForNextTask();

  bool ShuttingDown();

  // Back pointer to owning TaskScheduler.
  TaskScheduler *task_scheduler_;

  // Name of thread.
  std::string thread_name_;

  std::thread thread_;

  // Socket to talk to memcached. Not owned.
  //Socket *sock_;

};

} // namespace memcachedumper
