#pragma once

#include "utils/status.h"

#include <netinet/in.h>
#include <netdb.h>

#include <cstdint>
#include <string>

namespace memcachedumper {

class Sockaddr {
 public:
  Sockaddr();

  Sockaddr& operator=(const struct sockaddr_in &addr);

  const struct sockaddr_in& raw_struct_ref() const {
    return sockaddr_;
  }

  // Populate our sockaddr with the right resolved hostname and port.
  Status ResolveAndPopulateSockaddr(const std::string& hostname, int port);

 private:
  struct sockaddr_in sockaddr_;
};

} // namespace memcachedumper
