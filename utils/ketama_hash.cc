
#include "utils/ketama_hash.h"

namespace memcachedumper {

KetamaHasher::KetamaHasher(std::vector<std::string> rh)
  : hostmap_() {
  size_t n = rh.size();
  if (!n || n > furc_maximum_pool_size()) {
    throw std::logic_error("Pool size out of range for Ch3");
  }
  for (uint32_t i = 0; i < rh.size(); ++i)
  {
    std::vector<std::string> parts = StringUtil::Split(rh[i], ":");
    if (parts.size() < 2 ) {
        throw std::logic_error("Access point is not correctly formatted");
    }
    std::string hostname = parts[0];
    std::string port = parts[1];

    uint32_t bucketSize = 160; // TODO : read from the config
    for (uint32_t j = 0; j < bucketSize / 4; ++j) {
      std::stringstream hostStringStream;
      hostStringStream << hostname << "/" << hostname
        << ":" << port << "-" << j;
      std::string hostString = hostStringStream.str();
      unsigned char result[MD5_DIGEST_LENGTH];
      std::string md5str =
          MD5HashHelper(hostString.c_str(), hostString.size(), result,
              MD5_DIGEST_LENGTH);
      for (uint32_t k = 0; k < 4; ++k) {
        uint32_t hash = hashAsInt(result, k);
        hostmap_[hash] = i;
        idx_to_hostnames_[i] = hostname + ":" + port;
      }
    }
  }
}

uint32_t KetamaHasher::KetamaHash(const char* const key, const size_t len) const {
  unsigned char result[MD5_DIGEST_LENGTH];
  MD5HashHelper(key, len, result, MD5_DIGEST_LENGTH);
  uint32_t hash = hashAsInt(result, 0);

  auto serverIter = hostmap_.lower_bound(hash);
  if (serverIter == hostmap_.end()) {
    serverIter = hostmap_.begin();
  }
  uint32_t idx = serverIter->second;
  return idx;
}

} // namespace memcachedumper
