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
  KeyFilter(std::vector<std::string> all_ips, std::vector<std::string> dest_ips) {
    all_ips_ = all_ips;
    dest_ips_ = dest_ips;
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

  KetamaHasher* hasher_;

  // Set containing indicies of instances that we want to
  // filter for.
  std::unordered_set<uint32_t> filtered_instance_idxs_;
};

} // namespace memcachedumper
