#include "utils/key_filter.h"

namespace memcachedumper {

Status KeyFilter::Init() {
  try {
    hasher_ = new KetamaHasher(all_ips_, ketama_bucket_size_);
  } catch (...) {
    return Status::InvalidArgument("Destination IPs provided are invalid");
  }

  // TODO: Rewrite all_ips_ as a map so that we don't have to do
  // a nested loop. However, it's not the worst thing in the world as
  // we'll do this exactly once in the lifetime of the process.
  for (uint32_t i = 0; i < all_ips_.size(); ++i) {
    std::string hostname = hasher_->getHostnameByIdx(i);

    // We store the indices returned by the ketama hasher of instances that we
    // want to filter keys for.
    for (uint32_t j = 0; j < dest_ips_.size(); ++j) {
      if (hostname.compare(dest_ips_[j]) == 0) {
        filtered_instance_idxs_.insert(i);
      }
    }
  }

  return Status::OK();
}

bool KeyFilter::FilterKey(const std::string& key) {

  // Get index of host that 'key' should be hashed to.
  uint32_t idx = (*hasher_)(key);

  // If the host index doesn't exist in our set, then we return true to indicate
  // that the key must be filtered out.
  return (filtered_instance_idxs_.find(idx) == filtered_instance_idxs_.end()) ?
    true : false;
}

} // namespace memcachedumper
