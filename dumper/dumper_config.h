#pragma once

// Local project includes
#include "utils/status.h"

// Extern includes
#include "yaml-cpp/yaml.h"

// C++ includes
#include <string>

// YAML argument definitions
#define ARG_IP                        "ip"
#define ARG_PORT                      "port"
#define ARG_THREADS                   "threads"
#define ARG_BUFSIZE                   "bufsize"
#define ARG_MEMLIMIT                  "memlimit"
#define ARG_KEY_FILE_SIZE             "key_file_size"
#define ARG_DATA_FILE_SIZE            "data_file_size"
#define ARG_OUTPUT_DIR                "output_dir"
#define ARG_BULK_GET_THRESHOLD        "bulk_get_threshold"
#define ARG_ONLY_EXPIRE_AFTER_S       "only_expire_after_s"
#define ARG_CHECKPOINT_RESUME         "checkpoint_resume"
#define ARG_LOG_FILE_PATH             "log_file_path"
#define ARG_REQ_ID                    "req_id"
#define ARG_DEST_IPS                  "dest_ips"
#define ARG_ALL_IPS                   "all_ips"
#define ARG_IS_S3_DUMP                "is_s3_dump"
#define ARG_S3_BUCKET                 "s3_bucket"
#define ARG_S3_FINAL_PATH             "s3_final_path"

#define ENSURE_OPT_EXISTS(config, opt) do {                                 \
  if (!config[opt]) {                                                       \
    return Status::InvalidArgument("REQUIRED argument not provided", opt);  \
  }                                                                         \
} while (0);

namespace memcachedumper {

class DumperOptions;

class DumperConfig {
 public:
  // Load the configuration from the YAML file in 'config_filepath' and populate
  // the out-param 'opts'.
  // Returns an error Status if the configuration fails validation.
  static Status LoadConfig(std::string config_filepath, DumperOptions& opts);

 private:
  // Do a soft validation of the configuration.
  static Status ValidateConfig(const YAML::Node& config);
};

// Options to configure the cache dumper.
class DumperOptions {
 public:

  void set_config_file_path(std::string_view config_file_path);
  void set_memcached_hostname(std::string_view memcached_hostname);
  void set_memcached_port(int memcached_port);
  void set_num_threads(int num_threads);
  void set_chunk_size(uint64_t chunk_size);
  void set_bulk_get_threshold(uint32_t bulk_get_threshold);
  void set_max_memory_limit(uint64_t max_memory_limit);
  void set_max_key_file_size(uint64_t max_key_file_size);
  void set_max_data_file_size(uint64_t max_data_file_size);
  void set_log_file_path(std::string_view logfile_path);
  void set_output_dir_path(std::string_view output_dir_path);
  void set_only_expire_after(int only_expire_after);
  void set_resume_mode(bool resume_mode);
  void set_is_s3_dump(bool is_s3_dump);
  void set_s3_bucket_name(std::string s3_bucket);
  void set_s3_final_path(std::string s3_path);
  void set_req_id(std::string req_id);
  void set_dest_ips_filepath(std::string dest_ips_filepath);
  void set_all_ips_filepath(std::string all_ips_filepath);
  void add_dest_ip(const std::string& dest_ip);
  void add_all_ip(const std::string& all_ip);

  std::string config_file_path() { return config_file_path_; }
  std::string memcached_hostname() { return memcached_hostname_; }
  int memcached_port() { return memcached_port_; }
  int num_threads() { return num_threads_; }
  uint32_t bulk_get_threshold() { return bulk_get_threshold_; }
  uint64_t chunk_size() { return chunk_size_; }
  uint64_t max_memory_limit() { return max_memory_limit_; }
  uint64_t max_key_file_size() { return max_key_file_size_; }
  uint64_t max_data_file_size() { return max_data_file_size_; }
  std::string log_file_path() { return log_file_path_; }
  std::string output_dir_path() { return output_dir_path_; }
  int only_expire_after() { return only_expire_after_; }
  bool is_resume_mode() { return resume_mode_; }
  bool is_s3_dump() { return is_s3_dump_; }
  std::string s3_bucket() { return s3_bucket_; }
  std::string s3_path() { return s3_path_; }
  std::string req_id() { return req_id_; }
  const std::vector<std::string>& dest_ips() { return dest_ips_; }
  const std::vector<std::string>& all_ips() { return all_ips_; }

 private:
  // Path to configuration file.
  std::string config_file_path_;
  // Hostname that contains target memecached.
  std::string memcached_hostname_;
  // Memcached's port.
  int memcached_port_;
  // Number of worker threads for this cache dumper.
  int num_threads_;
  // Number of keys to bulk get.
  uint32_t bulk_get_threshold_ = 0;
  // The size to use for each allocated buffer (in bytes).
  uint64_t chunk_size_;
  // The maximum amount of memory to use (in bytes).
  uint64_t max_memory_limit_;
  // Max size per key file we produce.
  uint64_t max_key_file_size_;
  // Max size per data file we produce.
  uint64_t max_data_file_size_;
  // Path to the log file.
  std::string log_file_path_;
  // Path to the output directory.
  std::string output_dir_path_;
  // Ignore keys that expire within these many seconds.
  int only_expire_after_ = 0;
  // Indicates that we have to resume from a checkpoint and not start dumping
  // from scratch.
  bool resume_mode_;
  // Uploads to S3 if set.
  bool is_s3_dump_;
  // S3 Bucket name.
  std::string s3_bucket_;
  // S3 Final path.
  std::string s3_path_;
  // Dump request ID.
  std::string req_id_;
  // IP:port pairs of target instances that we want to narrow the dump to.
  std::vector<std::string> dest_ips_;
  // IP:port pairs of all instances in the target replica.
  std::vector<std::string> all_ips_;
};

} // namespace memcachedumper
