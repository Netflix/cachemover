#include "utils/mem_mgr.h"

#include <stdlib.h>

#include <iostream>

namespace memcachedumper {

MemoryManager::MemoryManager(uint64_t chunk_size, int num_chunks)
  : chunk_size_(chunk_size),
    num_chunks_(num_chunks) {
}

int MemoryManager::PreallocateChunks() {
  std::lock_guard<std::mutex> lock(list_mutex_);

  std::cout << "DEBUG: Going to allocate chunks" << std::endl;
  for (int i = 0; i < num_chunks_; ++i) {
    uint8_t *buf = static_cast<uint8_t*>(malloc(chunk_size_));
    if (buf == nullptr) return -1; // TODO: Replace with status code.

    free_buffers_.push_back(buf);
  }

  return 0;
}

MemoryManager::~MemoryManager() = default;

uint8_t* MemoryManager::GetBuffer() {
  std::lock_guard<std::mutex> lock(list_mutex_);

  if (free_buffers_.empty()) return nullptr;

  uint8_t *buf = free_buffers_.front();
  free_buffers_.pop_front();

  return buf;
}

void MemoryManager::ReturnBuffer(uint8_t *buf) {
  std::lock_guard<std::mutex> lock(list_mutex_);
  free_buffers_.push_back(buf);
}

} // namespace memcachedumper
