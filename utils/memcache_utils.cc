
#include "utils/memcache_utils.h"
#include "utils/net_util.h"

#include <iostream>
#include <sstream>


namespace memcachedumper {

McData::McData(char *key, size_t keylen, int32_t expiry)
  : key_(std::string(key, keylen)),
    expiry_(expiry),
    value_len_(0),
    get_complete_(false),
    complete_(false),
    get_attempts_(0) {
}

void McData::setValue(const char* data, size_t size) {
  data_.reset(new Slice(data, size));
}

void McData::printValue() {
  //std::cout << "McData: " << key_.c_str() << " -> " << data_->ToString() << std::endl;
}

std::string MemcachedUtils::output_dir_path_;

void MemcachedUtils::SetOutputDirPath(std::string output_dir_path) {
  MemcachedUtils::output_dir_path_ = output_dir_path;
}

std::string MemcachedUtils::GetKeyFilePath() {
  return MemcachedUtils::output_dir_path_ + "/keyfile/";
}
std::string MemcachedUtils::GetDataStagingPath() {
  return MemcachedUtils::output_dir_path_ + "/datafiles_staging/";
}
std::string MemcachedUtils::GetDataFinalPath() {
  return MemcachedUtils::output_dir_path_ + "/datafiles_completed/";
}

std::string MemcachedUtils::KeyFilePrefix() {
  std::string* ip_addr = nullptr;
  Status s = GetIPAddrAsString(&ip_addr);
  if (!s.ok()) {
    std::cout << "Could not get IP Address: " << s.ToString() << std::endl;
    // If we could not get the IP Address, use "localhost".
    // TODO: Might be confusing?
    return "key_localhost_";
  }
  std::string kprefix;
  kprefix.append("key_");
  kprefix.append(*ip_addr);
  kprefix.append("_");
  return kprefix;
}

std::string MemcachedUtils::DataFilePrefix() {
  std::string* ip_addr = nullptr;
  Status s = GetIPAddrAsString(&ip_addr);
  if (!s.ok()) {
    std::cout << "Could not get IP Address: " << s.ToString() << std::endl;
    // If we could not get the IP Address, use "localhost".
    // TODO: Might be confusing?
    return "data_localhost_";
  }
  std::string dprefix;
  dprefix.append("data_");
  dprefix.append(*ip_addr);
  dprefix.append("_");
  return dprefix;
}

std::string MemcachedUtils::CraftBulkGetCommand(
    McDataMap* pending_keys, const int max_keys) {
  std::stringstream bulk_get_cmd;
  bulk_get_cmd << "get ";
  int32_t num_keys_to_get = 0;

  McDataMap::iterator it = pending_keys->begin();
  while (it != pending_keys->end()) {
    if (!it->second->get_complete()) {
      bulk_get_cmd << it->first << " ";
      ++num_keys_to_get;
      if (num_keys_to_get == max_keys) break;
    }
    it++;
  }

  if (num_keys_to_get == 0) return std::string();

  bulk_get_cmd << "\n";
  return bulk_get_cmd.str();
}

} // namespace memcachedumper
