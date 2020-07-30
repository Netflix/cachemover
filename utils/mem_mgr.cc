#include "utils/mem_mgr.h"

#include <stdlib.h>

#include <iostream>

namespace memcachedumper {

MemoryManager::MemoryManager(uint64_t chunk_size, int num_chunks)
  : chunk_size_(chunk_size),
    num_chunks_(num_chunks) {
}

Status MemoryManager::PreallocateChunks() {
  std::lock_guard<std::mutex> lock(list_mutex_);
  std::cout << "Chunk size is " << chunk_size_ << std::endl;

  uint8_t* main_buff = static_cast<uint8_t*>(malloc(chunk_size_ * num_chunks_));

  if (main_buff == nullptr) {
      return Status::OutOfMemoryError("Could not pre-allocate chunks");
  }


  for (int i = 0; i < num_chunks_; ++i) {
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
  free_buffers_.push_back(buf);
}

} // namespace memcachedumper
