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

#include "utils/net_util.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>

namespace memcachedumper {

std::string cached_ip_addr;

Status GetIPAddrAsString(std::string** ip_addr) {
  char hostname[256]; 
  char *ip_raw_buf; 
  struct hostent *host_entry; 

  if (cached_ip_addr.empty()) {
    int ret = gethostname(hostname, sizeof(hostname));
    if (ret < 0) {
      return Status::NetworkError("Could not get hostname", strerror(errno));
    }

    host_entry = gethostbyname(hostname);
    if (host_entry == NULL) {
      return Status::NetworkError("gethostbyname() failed", hstrerror(h_errno));
    }

    ip_raw_buf = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0])); 

    cached_ip_addr = std::string(ip_raw_buf, strlen(ip_raw_buf));
  }

  *ip_addr = &cached_ip_addr;
  return Status::OK();
}

} // namespace memcachedumper
