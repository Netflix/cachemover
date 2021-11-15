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
#include "utils/socket.h"
#include "common/logger.h"

#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>

namespace memcachedumper {

#define RETRY_ON_EINTR(err, expr) do {                \
  static_assert(std::is_signed<decltype(err)>::value, \
                #err " must be a signed integer");    \
  (err) = (expr);                                     \
} while ((err) == -1 && errno == EINTR)

Socket::Socket()
  : fd_(-1), timeout_(1) {
}

Status Socket::Create() {
  fd_ = socket(AF_INET, SOCK_STREAM, 0);

  if (fd_ < 0) {
    return Status::NetworkError("Could not open socket", strerror(errno));
  }

  return Status::OK();
}

Status Socket::SetRecvTimeout(int seconds) {
  timeout_ = seconds;
  struct timeval tv;
  tv.tv_sec = seconds;
  tv.tv_usec = 0;
  if (setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1) {
    return Status::NetworkError("Could not set socket timeout", strerror(errno));
  }

  return Status::OK();
}

Status Socket::Connect(const Sockaddr& remote_addr) {
  struct sockaddr_in addr;

  if (fd_ == -1) {
    return Status::InvalidArgument("Socket not yet created. Cannot connect()");
  }

  // Copy before reinterpreting.
  //memcpy(&addr, &remote_addr.raw_struct_ref(), sizeof(sockaddr_in));
  remote_addr_ = remote_addr;
  int ret = connect(fd_,
      reinterpret_cast<const struct sockaddr*>(&remote_addr_.raw_struct_ref()),
      sizeof(addr));

  if (ret < 0) {
    return Status::NetworkError("Could not connect to host", strerror(errno));
  }

  return Status::OK();
}

Status Socket::Recv(uint8_t* buf, size_t len, int32_t *nbytes_read) {
  int32_t nbytes;
  int retry_count = 0;
  int pollin_happened = 0;
  pfds_[0].fd = fd_;
  pfds_[0].events = POLLIN;
  while (!pollin_happened && retry_count < 10) { // TODO make the retry count configurable
    int events = poll(pfds_, 1, timeout_ * 1000);
    if (events == 0) {
      LOG("Poll timed out, retrying..\n");
      retry_count++;
    } else {
      pollin_happened = pfds_[0].revents & POLLIN;
      if (!pollin_happened) {
        return Status::NetworkError("Socket timeout", strerror(pollin_happened));
      }
    }
  }

  RETRY_ON_EINTR(nbytes, recv(fd_, buf, len, 0));
  if (nbytes < 0) {
    return Status::NetworkError("Recv error", strerror(errno));
  } else if (nbytes == 0) {
    errno = ESHUTDOWN;
    return Status::NetworkError("Recv EOF", strerror(errno));
  }

  *nbytes_read = nbytes;
  return Status::OK();
}

Status Socket::Send(const uint8_t* buf, size_t len, int32_t *nbytes_sent) {
  int32_t nbytes;

  nbytes = send(fd_, buf, len, 0);
  if (nbytes < 0) {
    return Status::NetworkError("Send error", strerror(errno));
  }

  *nbytes_sent = nbytes;
  return Status::OK();
}

Status Socket::Close() {
  if (fd_ < 0) return Status::OK();

  int fd = fd_;
  int ret;
  RETRY_ON_EINTR(ret, close(fd));
  if (ret < 0) {
    return Status::NetworkError("Close error", strerror(errno));
  }
  fd_ = -1;
  return Status::OK();
}

Status Socket::Refresh() {
  RETURN_ON_ERROR(Close());
  RETURN_ON_ERROR(Create());
  // TODO: Make configurable if necessary.
  RETURN_ON_ERROR(SetRecvTimeout(2));

  int num_retries = 0;
  int sleep_duration_s = 5;
  Status connect_status = Status::OK();
  do {
    connect_status = Connect(remote_addr_);
    if (connect_status.ok()) return Status::OK();

    LOG("Connect() failed. Sleeping for {0} seconds.", sleep_duration_s);
    sleep(sleep_duration_s);
    // TODO: Make configurable
    sleep_duration_s += 10;
    ++num_retries;
  } while (num_retries < 3);

  return connect_status;
}

} // namespace memcachedumper
