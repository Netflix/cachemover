#include "utils/ring_buffer.h"

#include <assert.h>

namespace memcachedumper {

RingBuffer::RingBuffer(uint8_t* buffer, uint32_t capacity)
 : buffer_(buffer),
   capacity_(capacity),
   bytes_used_(0),
   head_(0),
   tail_(0) {
}

uint32_t RingBuffer::Write(uint8_t* data, size_t size) {
  assert(size <= FreeBytes());

  return 0;
}

} // namespace memcachedumper
