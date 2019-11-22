#pragma once

#include "utils/sockaddr.h"
#include "utils/socket.h"

#include <mutex>
#include <string>
#include <vector>

namespace memcachedumper {

class SocketPool {
 public:
  SocketPool(std::string_view hostname, int port, int num_sockets);

  int PrimeConnections();

  Socket* GetSocket();

  void ReleaseSocket();

 private:
  std::string hostname_;
  int port_;
  int num_sockets_;

  Sockaddr sockaddr_;
  std::vector<Socket*> sockets_;
  std::mutex mutex_;
};

} // namespace memcachedumper
