#pragma once

#include "utils/sockaddr.h"
#include "utils/status.h"

#include <cstddef>
#include <cstdint>
#include <poll.h>

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
  Status Close();
  Status Refresh();

 private:
  int fd_;
  Sockaddr remote_addr_;
  struct pollfd pfds_[1];
  int timeout_;
};

} // namespace memcachedumper
