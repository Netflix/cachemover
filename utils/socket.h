#pragma once

#include "utils/status.h"

#include <cstddef>
#include <cstdint>

namespace memcachedumper {

class Sockaddr;

class Socket {
 public:
  Socket();
  Status Create();
  Status SetRecvTimeout(int seconds);
  Status Connect(const Sockaddr& remote_addr);
  Status Recv(uint8_t* buf, size_t len, int32_t *nbytes_read);
  Status Send(const uint8_t* buf, size_t len, int32_t *nbytes_sent);

 private:
  int fd_;
};

} // namespace memcachedumper
