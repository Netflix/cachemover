
#include "utils/memcache_utils.h"

#include <iostream>

namespace memcachedumper {

McData::McData(char *key, size_t keylen, int32_t expiry)
  : key_(std::string(key, keylen)),
    expiry_(expiry),
    value_len_(0),
    complete_(false) {
}

void McData::setValue(const char* data, size_t size) {
  data_.reset(new Slice(data, size));
}

void McData::printValue() {
  //std::cout << "McData: " << key_.c_str() << " -> " << data_->ToString() << std::endl;
}

} // namespace memcachedumper
