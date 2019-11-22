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

int Socket::Recv(uint8_t* buf, size_t len) {
  int bytes;

  bytes = recv(fd_, buf, len, 0);
  if (bytes < 0) {
    std::cout << "Got recv error. Reason: " << strerror(errno) << std::endl;
    return bytes;
  } else if (bytes == 0) {
    std::cout << "Got recv EOF. Reason: " << strerror(errno) << std::endl;
    return bytes;
  }

  return bytes;
}

int Socket::Send(const uint8_t* buf, size_t len) {
  int bytes;

  bytes = send(fd_, buf, len, 0);
  if (bytes < 0) {
    std::cout << "Got send error. Reason: " << strerror(errno) << std::endl;
    return bytes;
  } else if (bytes == 0) {
    std::cout << "Got send EOF. Reason: " << strerror(errno) << std::endl;
    return bytes;
  }

  return bytes;
}

} // namespace memcachedumper
