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

int Sockaddr::ResolveAndPopulateSockaddr(const std::string& hostname, int port) {
  struct hostent *server;

  server = gethostbyname(hostname.c_str());
  if (server == nullptr) {
    std::cout << "ERROR: Could not find host " << hostname << std::endl;
    return -1;
  }
  memcpy(&sockaddr_.sin_addr.s_addr, server->h_addr, server->h_length);

  sockaddr_.sin_port = htons(port);

  return 0;
}

} // namespace memcachedumper
