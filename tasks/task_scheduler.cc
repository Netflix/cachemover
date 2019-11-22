#include "tasks/task_scheduler.h"

#include "dumper/dumper.h"
#include "tasks/task_thread.h"

#include <iostream>

namespace memcachedumper {

TaskScheduler::TaskScheduler(int num_threads, Dumper* dumper)
  : num_threads_(num_threads),
    dumper_(dumper),
    all_tasks_complete_(false) {
  threads_.reserve(num_threads_);
}

TaskScheduler::~TaskScheduler() = default;

int TaskScheduler::Init() {
  std::cout << "Initing task scheduler" << std::endl;
  for (int i = 0; i < num_threads_; ++i) {
    threads_.push_back(
        std::make_unique<TaskThread>(this, "task_thread_" + std::to_string(i + 1)));
  }
  return 0;
}

void TaskScheduler::SubmitTask(Task *task) {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  task_queue_.push(task);
  {
    std::lock_guard<std::mutex> mlock(metrics_mutex_);
    num_waiting_++;
  }
  queue_cv_.notify_one();
}

Task* TaskScheduler::WaitForNextTask() {
  std::unique_lock<std::mutex> lock(queue_mutex_);
  Task *next_task = GetNextTaskHelper();
  while (next_task == nullptr) {
    queue_cv_.wait(lock);
    if (AllTasksComplete()) {
      next_task = nullptr;
      break;
    }

    next_task = GetNextTaskHelper();
  }

  return next_task;
}

Task* TaskScheduler::GetNextTaskHelper() {
  if (task_queue_.empty()) {
    return nullptr;
  }

  Task* task_to_return = task_queue_.front();
  task_queue_.pop();

  {
    std::lock_guard<std::mutex> mlock(metrics_mutex_);
    num_waiting_--;
    num_running_++;
  }
  return task_to_return;
}

void TaskScheduler::MarkTaskComplete(Task *task) {
  std::lock_guard<std::mutex> mlock(metrics_mutex_);
  num_running_--;

  // If all tasks completed, notify everyone waiting on the task queue
  // and everyone waiting for all tasks to complete.
  if (num_running_ == 0 && num_waiting_ == 0) {
    all_tasks_complete_ = true;
    tasks_completed_cv_.notify_all();
    queue_cv_.notify_all();
  }
}

void TaskScheduler::WaitUntilTasksComplete() {
  std::unique_lock<std::mutex> mlock(metrics_mutex_);
  tasks_completed_cv_.wait(mlock);
}

Socket* TaskScheduler::GetMemcachedSocket() {
  return dumper_->GetMemcachedSocket();
}

} // namespace memcachedumper
