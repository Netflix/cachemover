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
