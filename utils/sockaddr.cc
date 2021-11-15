/**
 *
 *  Copyright 2018 Netflix, Inc.
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

#include "utils/sockaddr.h"

#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>

#include <iostream>

namespace memcachedumper {

Sockaddr::Sockaddr() {
  memset(&sockaddr_, 0, sizeof(struct sockaddr_in));
  sockaddr_.sin_family = AF_INET;
  sockaddr_.sin_addr.s_addr = INADDR_ANY;
}

Sockaddr& Sockaddr::operator=(const struct sockaddr_in &addr) {
  if (&addr == &sockaddr_) {
    return *this;
  }
  memcpy(&sockaddr_, &addr, sizeof(struct sockaddr_in));
  return *this;
}

Status Sockaddr::ResolveAndPopulateSockaddr(const std::string& hostname, int port) {
  struct hostent *server;

  server = gethostbyname(hostname.c_str());
  if (server == nullptr) {
    return Status::NetworkError("Could not find host", hostname);
  }
  memcpy(&sockaddr_.sin_addr.s_addr, server->h_addr, server->h_length);

  sockaddr_.sin_port = htons(port);

  return Status::OK();
}

} // namespace memcachedumper
