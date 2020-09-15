#include "common/logger.h"
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

McData::McData(std::string& key, int32_t expiry)
  : key_(key),
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
  LOG("McData: {0} -> {1}", key_.c_str(), data_->ToString());
}

// Static member declarations
std::string MemcachedUtils::req_id_;
std::string MemcachedUtils::output_dir_path_;
uint32_t MemcachedUtils::bulk_get_threshold_ = DEFAULT_BULK_GET_THRESHOLD;
uint64_t MemcachedUtils::max_data_file_size_;
int MemcachedUtils::only_expire_after_;
std::string MemcachedUtils::s3_bucket_;
std::string MemcachedUtils::s3_path_;
std::string MemcachedUtils::sqs_url_;
Aws::S3::S3Client* MemcachedUtils::s3_client_;
Aws::SQS::SQSClient* MemcachedUtils::sqs_client_;

void MemcachedUtils::SetReqId(std::string req_id) {
  MemcachedUtils::req_id_ = req_id;
}
void MemcachedUtils::SetOutputDirPath(std::string output_dir_path) {
  MemcachedUtils::output_dir_path_ = output_dir_path;
}
void MemcachedUtils::SetBulkGetThreshold(uint32_t bulk_get_threshold) {
  if (bulk_get_threshold == 0) {
    MemcachedUtils::bulk_get_threshold_ = DEFAULT_BULK_GET_THRESHOLD;
  } else {
    MemcachedUtils::bulk_get_threshold_ = bulk_get_threshold;
  }
}
void MemcachedUtils::SetMaxDataFileSize(uint64_t max_data_file_size) {
  MemcachedUtils::max_data_file_size_ = max_data_file_size;
}
void MemcachedUtils::SetOnlyExpireAfter(int only_expire_after) {
  MemcachedUtils::only_expire_after_ = only_expire_after;
}

void MemcachedUtils::SetS3Bucket(std::string s3_bucket) {
  MemcachedUtils::s3_bucket_ = s3_bucket;
}

void MemcachedUtils::SetS3Path(std::string s3_path) {
  MemcachedUtils::s3_path_ = s3_path;
}
void MemcachedUtils::SetSQSQueueURL(std::string sqs_url) {
  MemcachedUtils::sqs_url_ = sqs_url;
}

void MemcachedUtils::SetS3Client(Aws::S3::S3Client* s3_client) {
  MemcachedUtils::s3_client_ = s3_client;
}
void MemcachedUtils::SetSQSClient(Aws::SQS::SQSClient* sqs_client) {
  MemcachedUtils::sqs_client_ = sqs_client;
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
    LOG_ERROR("Could not get IP Address: {0}", s.ToString());
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
    LOG_ERROR("Could not get IP Address: {0}", s.ToString());
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
    McDataMap* pending_keys) {
  std::stringstream bulk_get_cmd;
  bulk_get_cmd << "get ";
  uint32_t num_keys_to_get = 0;

  McDataMap::iterator it = pending_keys->begin();
  while (it != pending_keys->end()) {
    if (!it->second->get_complete()) {
      bulk_get_cmd << it->first << " ";
      ++num_keys_to_get;
      if (num_keys_to_get == bulk_get_threshold_) break;
    }
    it++;
  }

  if (num_keys_to_get == 0) return std::string();

  bulk_get_cmd << "\n";
  return bulk_get_cmd.str();
}

} // namespace memcachedumper
