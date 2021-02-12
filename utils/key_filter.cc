#include "utils/key_filter.h"

namespace memcachedumper {

Status KeyFilter::Init() {
  try {
    hasher_ = new KetamaHasher(all_ips_);
  } catch (...) {
    return Status::InvalidArgument("Destination IPs provided are invalid");
  }

  // TODO: Rewrite all_ips_ as a map so that we don't have to do
  // a nested loop. However, it's not the worst thing in the world as
  // we'll do this exactly once in the lifetime of the process.
  for (uint32_t i = 0; i < all_ips_.size(); ++i) {
    std::string hostname = hasher_->getHostnameByIdx(i);
    for (uint32_t j = 0; j < dest_ips_.size(); ++j) {
      if (hostname.compare(dest_ips_[j]) == 0) {
        filtered_instance_idxs_.insert(i);
      }
    }
  }

  return Status::OK();
}

bool KeyFilter::FilterKey(const std::string& key) {
  uint32_t idx = (*hasher_)(key);
  if (filtered_instance_idxs_.find(idx) != filtered_instance_idxs_.end()) return true;
  return false;
}

} // namespace memcachedumper
