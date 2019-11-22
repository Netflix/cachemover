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
  int Recv(uint8_t* buf, size_t len);
  int Send(const uint8_t* buf, size_t len);

 private:
  int fd_;
};

} // namespace memcachedumper
