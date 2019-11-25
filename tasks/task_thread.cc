#include "tasks/task_thread.h"

#include "common/logger.h"
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
  LOG("[" + thread_name_ + "] Waiting for next task...");
  return task_scheduler_->WaitForNextTask();
}

void TaskThread::WorkerLoop() {
  LOG("Started thread: " + thread_name_);

  while (!ShuttingDown()) {
    Task *task = WaitForNextTask();

    if (task != nullptr) {
      LOG("[" + thread_name_ + "] Got next task");
      task->set_owning_thread(this);
      task->Execute();
      task_scheduler_->MarkTaskComplete(task);
    }
  }

  // TODO: There's a race here, the main thread can complete before this thread completes.
  LOG("Exiting thread: " + thread_name_);
}

} // namespace memcachedumper
