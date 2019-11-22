#include "utils/sockaddr.h"
#include "utils/socket.h"

#include <errno.h>
#include <string.h>
#include <sys/socket.h>

#include <iostream>

namespace memcachedumper {

Socket::Socket()
  : fd_(-1) {
}

Status Socket::Create() {
  fd_ = socket(AF_INET, SOCK_STREAM, 0);

  if (fd_ < 0) {
    return Status::NetworkError("Could not open socket", strerror(errno));
  }

  return Status::OK();
}

Status Socket::SetRecvTimeout(int seconds) {
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
  memcpy(&addr, &remote_addr.raw_struct_ref(), sizeof(sockaddr_in));
  int ret = connect(fd_, reinterpret_cast<const struct sockaddr*>(&addr),
      sizeof(addr));

  if (ret < 0) {
    return Status::NetworkError("Could not connect to host", strerror(errno));
  }

  return Status::OK();
}

Status Socket::Recv(uint8_t* buf, size_t len, int32_t *nbytes_read) {
  int32_t nbytes;

  nbytes = recv(fd_, buf, len, 0);
  if (nbytes < 0) {
    return Status::NetworkError("Recv error", strerror(errno));
  } else if (nbytes == 0) {
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

} // namespace memcachedumper
