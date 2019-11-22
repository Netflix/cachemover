#include "utils/socket_pool.h"

#include <iostream>

namespace memcachedumper {

SocketPool::SocketPool(std::string_view hostname, int port, int num_sockets)
  : hostname_(hostname),
    port_(port),
    num_sockets_(num_sockets) {
  sockets_.reserve(num_sockets_);
}

int SocketPool::PrimeConnections() {
  int ret;

  ret  = sockaddr_.ResolveAndPopulateSockaddr(hostname_, port_);
  if (ret < 0) return -1;

  for (int i = 0; i < num_sockets_; ++i) {
    sockets_.push_back(new Socket());
    Socket *sock = sockets_[i];
    ret = sock->Create();
    if (ret < 0) return -1;

    ret = sock->SetRecvTimeout(2);
    if (ret < 0) return -1;
    
    ret = sock->Connect(sockaddr_);
    if (ret < 0) return -1;
  }

  std::cout << "Successfully primed connections." << std::endl;

  return 0;
}

Socket* SocketPool::GetSocket() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (sockets_.empty()) return nullptr;

  Socket* sock = sockets_.back();
  sockets_.pop_back();
  return sock;
}

} // namespace memcachedumper
