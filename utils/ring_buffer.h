#pragma once

#include <stdint.h>

#include <cstddef>

namespace memcachedumper {

class RingBuffer {
 public:
  RingBuffer(uint8_t* buffer, uint32_t capacity);

  uint32_t FreeBytes() { return capacity_ - bytes_used_; }
  uint32_t BytesUsed() { return bytes_used_; }
  uint32_t Capacity() { return capacity_; }

  uint32_t Write(uint8_t* data, size_t size);

 private:
  uint8_t* buffer_;

  uint32_t capacity_;
  uint32_t bytes_used_;

  uint32_t head_;
  uint32_t tail_;
};

} // namespace memcachedumper
