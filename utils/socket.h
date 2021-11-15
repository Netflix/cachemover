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
