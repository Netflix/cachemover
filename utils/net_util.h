#pragma once

#include "utils/status.h"

#include <string>

namespace memcachedumper {

Status GetIPAddrAsString(const std::string* ip_addr);

} // namespace memcachedumper

