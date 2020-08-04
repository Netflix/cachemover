#pragma once

#include "utils/sockaddr.h"
#include "utils/socket.h"
#include "utils/status.h"

#include <mutex>
#include <string>
#include <vector>

namespace memcachedumper {

class SocketPool {
 public:
  SocketPool(std::string_view hostname, int port, int num_sockets);

  Status PrimeConnections();

  Socket* GetSocket();

  void ReleaseSocket(Socket *sock);

 private:
  std::string hostname_;
  int port_;
  size_t num_sockets_;

  Sockaddr sockaddr_;
  std::vector<Socket*> sockets_;
  std::mutex mutex_;
};

} // namespace memcachedumper
