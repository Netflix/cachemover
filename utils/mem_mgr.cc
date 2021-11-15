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

#include "common/logger.h"
#include "utils/mem_mgr.h"

#include <stdlib.h>

#include <iostream>

namespace memcachedumper {

MemoryManager::MemoryManager(uint64_t chunk_size, int num_chunks)
  : chunk_size_(chunk_size),
    num_chunks_(num_chunks) {
}

Status MemoryManager::PreallocateChunks() {

  uint8_t* main_buff = static_cast<uint8_t*>(malloc(chunk_size_ * num_chunks_));

  if (main_buff == nullptr) {
      return Status::OutOfMemoryError("Could not pre-allocate chunks");
  }

  for (size_t i = 0; i < num_chunks_; ++i) {
    uint8_t *buf = main_buff;
    free_buffers_.push_back(buf);
    main_buff += chunk_size_ + 1;
  }

  return Status::OK();
}

MemoryManager::~MemoryManager() = default;

uint8_t* MemoryManager::GetBuffer() {
  std::lock_guard<std::mutex> lock(list_mutex_);

  if (free_buffers_.empty()) return nullptr;

  uint8_t *buf = free_buffers_.back();
  free_buffers_.pop_back();
  return buf;
}

void MemoryManager::ReturnBuffer(uint8_t *buf) {
  std::lock_guard<std::mutex> lock(list_mutex_);
  assert(free_buffers_.size() < num_chunks_);
  free_buffers_.push_back(buf);
}

} // namespace memcachedumper
