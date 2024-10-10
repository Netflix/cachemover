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

#include "utils/ketama_hash.h"
#include "utils/status.h"
#include "utils/string_util.h"

#include <unordered_set>

class KetamaHasher;

namespace memcachedumper {

class KeyCleaner {
 public:
  KeyCleaner(std::vector<std::string> all_ips,
    uint32_t ketama_bucket_size) {
    all_ips_ = all_ips;
    ketama_bucket_size_ = ketama_bucket_size;
  }

  // Initializes the KeyCleaner.
  Status Init();

  // Returns 'true' if 'key' should be cleaned out; 'false' otherwise.
  bool CheckIfKeyIsOrphan(const std::string& key, const std::string& current_ip);

 private:
  // List of all IP addresses
  std::vector<std::string> all_ips_;
  // Bucket size to configure the ketama hasher with.
  uint32_t ketama_bucket_size_;

  KetamaHasher* cleanup_hasher_;
};

} // namespace memcachedumper
