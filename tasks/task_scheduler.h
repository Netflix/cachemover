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

#pragma once

#include "tasks/task.h"

#include <atomic>
#include <condition_variable>
#include <queue>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace memcachedumper {

class Dumper;
class Socket;
class TaskThread;

class TaskScheduler {
 public:
  TaskScheduler(int num_threads, Dumper* dumper);

  ~TaskScheduler();

  MemoryManager* mem_mgr();

  Dumper* dumper() { return dumper_; }

  int Init();

  void SubmitTask(Task *task);

  Task* WaitForNextTask();

  void WaitUntilTasksComplete();

  // Obtain the latest metrics from task threads and update the dump metrics.
  void UpdateMetrics();

  // Get a socket to memcached.
  Socket* GetMemcachedSocket();

  // Release a memcached socket.
  void ReleaseMemcachedSocket(Socket *sock);

 private:
  friend class TaskThread;

  // Returns the next task in the queue of it's present.
  // Returns nullptr otherwise. Not thread-safe.
  Task* GetNextTaskHelper();

  // Returns true if 'all_tasks_compete_' is true.
  bool AllTasksComplete() { return all_tasks_complete_; }

  void MarkTaskComplete(Task *task);

  // A queue of tasks to work on.
  std::queue<Task*> task_queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;

  std::vector<std::unique_ptr<TaskThread>> threads_;
  int num_threads_;

  // Number of tasks currently running.
  uint32_t num_running_;

  // Number of tasks waiting in the queue.
  uint32_t num_waiting_;

  std::mutex metrics_mutex_;
  std::condition_variable tasks_completed_cv_;

  // Back pointer to the owning Dumper class.
  Dumper* dumper_;

  // If this is true, we've been asked to shut down.
  std::atomic<bool> all_tasks_complete_;

};

} // memcachedumper
