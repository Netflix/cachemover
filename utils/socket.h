#pragma once

#include <cstddef>
#include <cstdint>

namespace memcachedumper {

class Sockaddr;

class Socket {
 public:
  Socket();
  int Create();
  int SetRecvTimeout(int seconds);
  int Connect(const Sockaddr& remote_addr);
  int Recv(uint8_t* buf, size_t len);
  int Send(const uint8_t* buf, size_t len);

 private:
  int fd_;
};

} // namespace memcachedumper
