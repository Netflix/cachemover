#include "utils/socket_pool.h"

#include <iostream>

namespace memcachedumper {

SocketPool::SocketPool(std::string_view hostname, int port, int num_sockets)
  : hostname_(hostname),
    port_(port),
    num_sockets_(num_sockets) {
  sockets_.reserve(num_sockets_);
}

Status SocketPool::PrimeConnections() {
  Status status  = sockaddr_.ResolveAndPopulateSockaddr(hostname_, port_);
  if (!status.ok()) return status;

  for (int i = 0; i < num_sockets_; ++i) {
    sockets_.push_back(new Socket());
    Socket *sock = sockets_[i];
    status = sock->Create();
    if (!status.ok()) return status;

    status = sock->SetRecvTimeout(2);
    if (!status.ok()) return status;
    
    status = sock->Connect(sockaddr_);
    if (!status.ok()) {
      return status;
    }
  }

  std::cout << "Successfully primed connections." << std::endl;

  return Status::OK();
}

Socket* SocketPool::GetSocket() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (sockets_.empty()) return nullptr;

  Socket* sock = sockets_.back();
  sockets_.pop_back();
  return sock;
}

} // namespace memcachedumper
