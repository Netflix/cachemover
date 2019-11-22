#include "tasks/task_thread.h"

#include "tasks/task.h"
#include "tasks/task_scheduler.h"
#include "utils/socket.h"

#include <unistd.h>

#include <iostream>

namespace memcachedumper {

TaskThread::TaskThread(TaskScheduler *task_scheduler, const std::string& thread_name)
  : task_scheduler_(task_scheduler),
    thread_name_(thread_name),
    thread_(&TaskThread::WorkerLoop, this) {
}

TaskThread::~TaskThread() {
  if (thread_.joinable()) thread_.join();
}

bool TaskThread::ShuttingDown() {
  return task_scheduler_->AllTasksComplete();
}

Task* TaskThread::WaitForNextTask() {
  std::cout << "Waiting for next task. Thread: " << thread_name_ << std::endl;
  return task_scheduler_->WaitForNextTask();
}

void TaskThread::WorkerLoop() {
  std::cout << "Started thread " << thread_name_ << std::endl;

  while (!ShuttingDown()) {
    Task *task = WaitForNextTask();

    if (task != nullptr) {
      std::cout << "\tGot next task" << std::endl;
      task->set_owning_thread(this);
      task->Execute();
      task_scheduler_->MarkTaskComplete(task);
    }
  }

  // TODO: There's a race here, the main thread can complete before this thread completes.
  std::cout << "\t\tExiting thread: " << thread_name_ << std::endl;
}

} // namespace memcachedumper
