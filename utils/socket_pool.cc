#include "utils/socket_pool.h"

#include "common/logger.h"

#include <iostream>
#include <sstream>

namespace memcachedumper {

SocketPool::SocketPool(std::string_view hostname, int port, int num_sockets)
  : hostname_(hostname),
    port_(port),
    num_sockets_(num_sockets) {
  sockets_.reserve(num_sockets_);
}

Status SocketPool::PrimeConnections() {
  RETURN_ON_ERROR(sockaddr_.ResolveAndPopulateSockaddr(hostname_, port_));

  for (int i = 0; i < num_sockets_; ++i) {
    sockets_.push_back(new Socket());
    Socket *sock = sockets_[i];
    RETURN_ON_ERROR(sock->Create());

    // TODO: Make configurable if necessary.
    RETURN_ON_ERROR(sock->SetRecvTimeout(2));
    RETURN_ON_ERROR(sock->Connect(sockaddr_));
  }

  {
    std::stringstream ss;
    ss << "Successfully primed " << num_sockets_ << " connections";
    LOG(ss.str());
  }
  return Status::OK();
}

Socket* SocketPool::GetSocket() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (sockets_.empty()) {
    return nullptr;
  }

  Socket* sock = sockets_.back();
  sockets_.pop_back();
  return sock;
}

void SocketPool::ReleaseSocket(Socket *sock) {
  std::lock_guard<std::mutex> lock(mutex_);
  sockets_.push_back(sock);
}

} // namespace memcachedumper
