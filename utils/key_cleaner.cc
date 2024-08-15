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

#include "utils/key_cleaner.h"
#include "common/logger.h"

namespace memcachedumper {

Status KeyCleaner::Init() {
  try {
    cleanup_hasher_ = new KetamaHasher(all_ips_, ketama_bucket_size_);
  } catch (...) {
    return Status::InvalidArgument("IPs provided are invalid");
  }

  return Status::OK();
}

bool KeyCleaner::CheckIfKeyIsOrphan(const std::string& key, const std::string& current_ip) {
  // Get index of host that 'key' should be hashed to.
  uint32_t idx = (*cleanup_hasher_)(key);
  std::string hostname = cleanup_hasher_->getHostnameByIdx(idx);
  hostname = hostname.substr(0, hostname.find(':'));
  return (hostname != current_ip) ? true : false;
}


} // namespace memcachedumper
