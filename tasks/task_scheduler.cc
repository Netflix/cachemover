#include "common/logger.h"
#include "dumper/dumper.h"
#include "tasks/task_scheduler.h"
#include "tasks/task_thread.h"

#include <chrono>
#include <iostream>
#include <sstream>

namespace memcachedumper {

TaskScheduler::TaskScheduler(int num_threads, Dumper* dumper)
  : num_threads_(num_threads),
    num_running_(0),
    num_waiting_(0),
    dumper_(dumper),
    all_tasks_complete_(false) {
  threads_.reserve(num_threads_);
}

TaskScheduler::~TaskScheduler() = default;

int TaskScheduler::Init() {
  LOG("Initing task scheduler");
  for (int i = 0; i < num_threads_; ++i) {
    threads_.push_back(
        std::make_unique<TaskThread>(this, "task_thread_" + std::to_string(i + 1)));
  }
  return 0;
}

MemoryManager* TaskScheduler::mem_mgr() {
  return dumper_->mem_mgr();
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

  delete task;
  // If all tasks completed, notify everyone waiting on the task queue
  // and everyone waiting for all tasks to complete.
  if (num_running_ == 0 && num_waiting_ == 0) {
    all_tasks_complete_ = true;
    tasks_completed_cv_.notify_all();
    queue_cv_.notify_all();
  }
}

std::string TaskScheduler::MetricsAsString() {
  std::stringstream ss;
  ss  << "Metrics -> Dumped: " << total_keys_processed()
      << ". Not found: " << total_keys_ignored()
      << ". Skipped (expires soon): " << total_keys_missing() << std::endl;

  return ss.str();
}

void TaskScheduler::WaitUntilTasksComplete() {
  std::chrono::minutes min(1);
  std::unique_lock<std::mutex> mlock(metrics_mutex_);
  tasks_completed_cv_.wait(mlock);
}

Socket* TaskScheduler::GetMemcachedSocket() {
  return dumper_->GetMemcachedSocket();
}

void TaskScheduler::ReleaseMemcachedSocket(Socket *sock) {
  return dumper_->ReleaseMemcachedSocket(sock);
}

uint64_t TaskScheduler::total_keys_processed() {
  uint64_t total_keys_processed = 0;
  for (auto&& thread : threads_) {
    total_keys_processed += thread->num_keys_processed();
  }
  return total_keys_processed;
}

uint64_t TaskScheduler::total_keys_ignored() {
  uint64_t total_keys_ignored = 0;
  for (auto&& thread : threads_) {
    total_keys_ignored += thread->num_keys_ignored();
  }
  return total_keys_ignored;
}

uint64_t TaskScheduler::total_keys_missing() {
  uint64_t total_keys_missing = 0;
  for (auto&& thread : threads_) {
    total_keys_missing += thread->num_keys_missing();
  }
  return total_keys_missing;
}
} // namespace memcachedumper
