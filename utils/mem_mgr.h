#pragma once

#include "utils/status.h"

#include <stdint.h>

#include <mutex>
#include <list>

namespace memcachedumper {

class MemoryManager {
 public:
  MemoryManager(uint64_t chunk_size, int num_chunks);

  ~MemoryManager();

  uint64_t chunk_size() { return chunk_size_; }

  // Preallocates all the free buffers.
  Status PreallocateChunks();

  // Obtain a single chunk. Returns 'nullptr' if none are available.
  uint8_t* GetBuffer();

  // Return a buffer back into the free list.
  void ReturnBuffer(uint8_t *buf);

 private:

  // Size of each chunk.
  uint64_t chunk_size_;

  // Total number of chunks in this allocator.
  int num_chunks_;

  // A list of free buffers
  std::list<uint8_t*> free_buffers_;

  // Mutex to protect 'free_buffers_' list.
  std::mutex list_mutex_;
};

} // namespace memcachedumper
