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

#pragma once

#include "utils/ketama_hash.h"
#include "utils/status.h"
#include "utils/string_util.h"

#include <unordered_set>

class KetamaHasher;

namespace memcachedumper {

/// The 'KeyFilter' class allows filtering of keys through Ketama hashing.
/// As EVCache filters keys based on IP addresses of the hosts, the KeyFilter
/// class takes as input the IP addresses of all the hosts in an ASG, and also
/// the subset of those hosts that we want to filter keys for.
class KeyFilter {
 public:
  KeyFilter(std::vector<std::string> all_ips, std::vector<std::string> dest_ips,
      uint32_t ketama_bucket_size) {
    all_ips_ = all_ips;
    dest_ips_ = dest_ips;
    ketama_bucket_size_ = ketama_bucket_size;
  }

  // Initializes the KeyFilter.
  Status Init();

  // Returns 'true' if 'key' should be filtered out; 'false' otherwise.
  bool FilterKey(const std::string& key);

 private:
  // List of all IP addresses in our target ASG.
  std::vector<std::string> all_ips_;
  // List of destination IPs that we want to filter for.
  std::vector<std::string> dest_ips_;
  // Bucket size to configure the ketama hasher with.
  uint32_t ketama_bucket_size_;

  KetamaHasher* hasher_;

  // Set containing indicies of instances that we want to
  // filter for.
  std::unordered_set<uint32_t> filtered_instance_idxs_;
};

} // namespace memcachedumper
