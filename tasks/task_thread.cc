/**
 *
 *  Copyright 2018 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

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
    thread_(&TaskThread::WorkerLoop, this),
    num_keys_processed_(0),
    num_keys_ignored_(0),
    num_keys_missing_(0),
    num_keys_filtered_(0) {
}

TaskThread::~TaskThread() {
  if (thread_.joinable()) thread_.join();
}

MemoryManager* TaskThread::mem_mgr() {
  return task_scheduler_->mem_mgr();
}

bool TaskThread::ShuttingDown() {
  return task_scheduler_->AllTasksComplete();
}

Task* TaskThread::WaitForNextTask() {
  return task_scheduler_->WaitForNextTask();
}

void TaskThread::WorkerLoop() {
  while (!ShuttingDown()) {
    Task *task = WaitForNextTask();

    if (task != nullptr) {
      task->set_owning_thread(this);
      task->Execute();
      task_scheduler_->MarkTaskComplete(task);
    }
  }

  // TODO: There's a race here, the main thread can complete before this thread completes.
  LOG("Exiting thread...");
}

} // namespace memcachedumper
