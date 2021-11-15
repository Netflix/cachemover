/**
 *
 *  Copyright 2021 Netflix, Inc.
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
  DoneTask(uint64_t total,
      uint64_t dumped,
      uint64_t skipped,
      uint64_t not_found,
      uint64_t filtered,
      std::string time_taken_str);
  ~DoneTask();

  void Execute() override;

 private:
  uint64_t total_;
  uint64_t dumped_;
  uint64_t skipped_;
  uint64_t not_found_;
  uint64_t filtered_;
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
