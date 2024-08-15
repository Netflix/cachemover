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

#include "tasks/task.h"
#include "utils/status.h"

#include <string>

namespace memcachedumper {

class MemoryManager;
class Socket;

class CleanupTask : public Task {
 public:
  CleanupTask(MemoryManager *mem_mgr, std::string current_ip, bool enable_hash_table_crawl);
  ~CleanupTask() = default;

  void Execute() override;

 private:
  Status SendCommand(const std::string& cleanup_cmd);
  Status RecvResponse();
  void CleanupOrphanKeys(std::string chunk_keys);

  Socket *memcached_socket_;

  // Pointer to a memory manager.
  MemoryManager *mem_mgr_;

  std::string current_ip_;

  bool enable_hash_table_crawl_;

};

} // namespace memcachedumper
