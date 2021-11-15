/**
 *
 *  Copyright 2021 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

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

  for (size_t i = 0; i < num_sockets_; ++i) {
    sockets_.push_back(new Socket());
    Socket *sock = sockets_[i];
    RETURN_ON_ERROR(sock->Create());

    // TODO: Make configurable if necessary.
    RETURN_ON_ERROR(sock->SetRecvTimeout(2));
    RETURN_ON_ERROR(sock->Connect(sockaddr_));
  }

  {
    std::stringstream ss;
    LOG("Successfully primed {0} connections", num_sockets_);
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
  assert(sockets_.size() < num_sockets_);
  sockets_.push_back(sock);
}

} // namespace memcachedumper
