#pragma once

#include "common/logger.h"
#include "utils/key_filter.h"
#include "utils/string_util.h"

#include <map>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>

#include <openssl/md5.h>

#define FURC_SHIFT 23

using namespace std;

namespace memcachedumper {

inline uint32_t furc_maximum_pool_size(void) {
  return (1 << FURC_SHIFT);
}

class KetamaHasher {
 public:
  KetamaHasher(std::vector<std::string> rh, uint32_t bucket_size);

  // Returns the ketama hash of 'key'.
  uint32_t KetamaHash(const char* const key, const size_t len) const;
  size_t operator()(const std::string& hashable) const {
    return KetamaHash(hashable.c_str(), hashable.length());
  }

 std::string& getHostnameByIdx(uint32_t idx) {
   return idx_to_hostnames_[idx];
 }

 private:
  static uint32_t hashAsInt(unsigned char* result, uint32_t index) {
    return ((uint32_t)  (result[3 +  index * 4] & 0XFF) << 24 |
                              (uint32_t) (result[2 +  index * 4] & 0XFF) << 16 |
                              (uint32_t) (result[1 +  index * 4] & 0XFF) << 8 |
                              (uint32_t) (result[index * 4] & 0XFF));
  }

  static std::string MD5HashHelper(const char* const key, size_t keylen,
      unsigned char* result, unsigned int len) {
    std::string kHexBytes = "0123456789abcdef";
    MD5(reinterpret_cast<const unsigned char*>(key),
        keylen,
        result);

    std::string ret;
    for (unsigned int i = 0; i < len; ++i) {
      // Hex should print, for this, in a weird bytewise order:
      // If the bytes are b0b1b2b3..., we print:
      //       hi(b0) lo(b0) hi(b1) lo(b1) ...
      // Where hi(x) is the hex char of its upper 4 bits, and lo(x) is lower 4
      ret += kHexBytes[(result[i] >> 4) & 0x0F];
      ret += kHexBytes[result[i] & 0x0F];
    }
    return ret;
  }

  // Maintains an ordered map of hashes to host IDs.
  std::map<uint32_t, uint32_t> hostmap_;
  // Maintains a map of host IDs to hostnames.
  std::unordered_map<uint32_t, std::string> idx_to_hostnames_;
};

} // namespace memcachedumper
