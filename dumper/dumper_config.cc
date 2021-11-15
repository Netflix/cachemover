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

// Class header
#include "dumper/dumper_config.h"

// Local project includes
#include "common/logger.h"

// C++ includes
#include <iostream>

namespace memcachedumper {

Status DumperConfig::ValidateConfig(const YAML::Node& config) {

  LOG("Validating configuration...");

  // Ensure that REQUIRED arguments are provided.
  ENSURE_OPT_EXISTS(config, ARG_IP);
  ENSURE_OPT_EXISTS(config, ARG_PORT);
  ENSURE_OPT_EXISTS(config, ARG_THREADS);
  ENSURE_OPT_EXISTS(config, ARG_BUFSIZE);
  ENSURE_OPT_EXISTS(config, ARG_MEMLIMIT);
  ENSURE_OPT_EXISTS(config, ARG_KEY_FILE_SIZE);
  ENSURE_OPT_EXISTS(config, ARG_DATA_FILE_SIZE);
  ENSURE_OPT_EXISTS(config, ARG_OUTPUT_DIR);
  ENSURE_OPT_EXISTS(config, ARG_LOG_FILE_PATH);
  ENSURE_OPT_EXISTS(config, ARG_REQ_ID);

  if (!config[ARG_IP].IsScalar()) {
    return Status::InvalidArgument(
        "Bad 'ip' argument", config[ARG_IP].as<std::string>());
  }
  if (!config[ARG_PORT].IsScalar() ||
      !(config[ARG_PORT].as<uint32_t>() > 9000 &&
      config[ARG_PORT].as<uint32_t>() < 65535)) {
    return Status::InvalidArgument(
        "Bad 'port' argument", config[ARG_PORT].as<std::string>());
  }

  uint16_t num_threads = config[ARG_THREADS].as<uint16_t>();
  uint64_t bufsize = config[ARG_BUFSIZE].as<uint64_t>();
  uint64_t memlimit = config[ARG_MEMLIMIT].as<uint64_t>();
  if (!(num_threads > 0 && num_threads < 49)) {
    return Status::InvalidArgument(
        "Bad 'threads' argument", std::to_string(num_threads));
  }
  if (bufsize == 0) {
    return Status::InvalidArgument(
        "Bad 'bufsize' argument", std::to_string(bufsize));
  }
  if (memlimit == 0) {
    return Status::InvalidArgument(
        "Bad 'memlimit' argument", std::to_string(memlimit));
  }

  if (static_cast<uint64_t>(num_threads * 2 * bufsize) > memlimit) {
    LOG_ERROR("Configuration error: Memory given is not enough for all threads.\n\
        Given: {0} bytes. \
        Required: {1} bytes for {2} buffers of size {3} (2 buffers per thread)",
            memlimit, num_threads * 2 * bufsize, 2 * num_threads, bufsize);
    return Status::InvalidArgument(
        "Insufficient 'memlimit' w.r.t 'num_threads' and 'bufsize'.");
  }

  if (!(config[ARG_KEY_FILE_SIZE].as<uint64_t>() > 0)) {
    return Status::InvalidArgument(
        "Bad 'key_file_size' argument", config[ARG_KEY_FILE_SIZE].as<std::string>());
  }
  if (!(config[ARG_DATA_FILE_SIZE].as<uint64_t>() > 0)) {
    return Status::InvalidArgument(
        "Bad 'data_file_size' argument", config[ARG_DATA_FILE_SIZE].as<std::string>());
  }
  if (config[ARG_OUTPUT_DIR].as<std::string>().empty()) {
    return Status::InvalidArgument("'output_dir' argument required");
  }
  if (!(config[ARG_BULK_GET_THRESHOLD].as<uint64_t>() > 0)) {
    return Status::InvalidArgument(
        "Bad 'bulk_get_threshold' argument",
        config[ARG_BULK_GET_THRESHOLD].as<std::string>());
  }
  if (!(config[ARG_ONLY_EXPIRE_AFTER_S].as<uint64_t>() >= 0)) {
    return Status::InvalidArgument(
        "Bad 'only_expire_after_s' argument",
        config[ARG_ONLY_EXPIRE_AFTER_S].as<std::string>());
  }
  if (config[ARG_LOG_FILE_PATH].as<std::string>().empty()) {
    return Status::InvalidArgument(
        "'log_file_path' argument required.");
  }
  if (config[ARG_REQ_ID].as<std::string>().empty()) {
    return Status::InvalidArgument("'req_id' argument required.");
  }

  if (config[ARG_DEST_IPS]) {
    if (!config[ARG_ALL_IPS]) {
      return Status::InvalidArgument("Must provide 'all_ips' along with 'dest_ips'");
    }
  }

  if (config[ARG_IS_S3_DUMP]) {
    if (config[ARG_IS_S3_DUMP].as<bool>() == true) {
      if (config[ARG_S3_BUCKET].as<std::string>().empty() ||
          config[ARG_S3_FINAL_PATH].as<std::string>().empty() ||
          config[ARG_SQS_QUEUE].as<std::string>().empty()) {
        return Status::InvalidArgument(
            "Must provide 's3_bucket', 's3_final_path' and 'sqs_queue'.");
      }
    }
  }
  return Status::OK();
}

Status DumperConfig::LoadConfig(std::string config_filepath, DumperOptions& out_opts) {
  YAML::Node config = YAML::LoadFile(config_filepath);
  RETURN_ON_ERROR(ValidateConfig(config));

  out_opts.set_memcached_hostname(config[ARG_IP].as<std::string>());
  out_opts.set_memcached_port(config[ARG_PORT].as<uint32_t>());
  out_opts.set_num_threads(config[ARG_THREADS].as<int>());
  out_opts.set_chunk_size(config[ARG_BUFSIZE].as<uint64_t>());
  out_opts.set_max_memory_limit(config[ARG_MEMLIMIT].as<uint64_t>());
  out_opts.set_max_key_file_size(config[ARG_KEY_FILE_SIZE].as<uint64_t>());
  out_opts.set_max_data_file_size(config[ARG_DATA_FILE_SIZE].as<uint64_t>());
  out_opts.set_output_dir_path(config[ARG_OUTPUT_DIR].as<std::string>());
  out_opts.set_bulk_get_threshold(config[ARG_BULK_GET_THRESHOLD].as<uint32_t>());
  out_opts.set_only_expire_after(config[ARG_ONLY_EXPIRE_AFTER_S].as<int>());
  out_opts.set_log_file_path(config[ARG_LOG_FILE_PATH].as<std::string>());
  out_opts.set_req_id(config[ARG_REQ_ID].as<std::string>());

  if (config[ARG_CHECKPOINT_RESUME]) {
    out_opts.set_resume_mode(config[ARG_CHECKPOINT_RESUME].as<bool>());
  }

  if (config[ARG_KETAMA_BUCKET_SIZE]) {
    out_opts.set_ketama_bucket_size(config[ARG_KETAMA_BUCKET_SIZE].as<uint32_t>());
  }
  for (auto dip : config[ARG_DEST_IPS]) {
    out_opts.add_dest_ip(dip.as<std::string>());
  }
  for (auto aip : config[ARG_ALL_IPS]) {
    out_opts.add_all_ip(aip.as<std::string>());
  }

  if (config[ARG_IS_S3_DUMP]) {
    if (config[ARG_IS_S3_DUMP].as<bool>() == true) {
      out_opts.set_is_s3_dump(true);
      out_opts.set_s3_bucket_name(config[ARG_S3_BUCKET].as<std::string>());
      out_opts.set_s3_final_path(config[ARG_S3_FINAL_PATH].as<std::string>());
      out_opts.set_sqs_queue_name(config[ARG_SQS_QUEUE].as<std::string>());
    } else {
      out_opts.set_is_s3_dump(false);
    }
  }
  return Status::OK();
}

void DumperOptions::set_config_file_path(std::string_view config_file_path) {
  config_file_path_ = config_file_path;
}

void DumperOptions::set_memcached_hostname(std::string_view memcached_hostname) {
  memcached_hostname_ = memcached_hostname;
}

void DumperOptions::set_memcached_port(int memcached_port) {
  memcached_port_ = memcached_port;
}

void DumperOptions::set_num_threads(int num_threads) {
  num_threads_ = num_threads;
}

void DumperOptions::set_chunk_size(uint64_t chunk_size) {
  chunk_size_ = chunk_size;
}

void DumperOptions::set_max_memory_limit(uint64_t max_memory_limit) {
  max_memory_limit_ = max_memory_limit;
}

void DumperOptions::set_max_key_file_size(uint64_t max_key_file_size) {
  max_key_file_size_ = max_key_file_size;
}

void DumperOptions::set_max_data_file_size(uint64_t max_data_file_size) {
  max_data_file_size_ = max_data_file_size;
}

void DumperOptions::set_log_file_path(std::string_view log_file_path) {
  log_file_path_ = log_file_path;
}

void DumperOptions::set_output_dir_path(std::string_view output_dir_path) {
  output_dir_path_ = output_dir_path;
}

void DumperOptions::set_bulk_get_threshold(uint32_t bulk_get_threshold) {
  bulk_get_threshold_ = bulk_get_threshold;
}

void DumperOptions::set_only_expire_after(int only_expire_after) {
  only_expire_after_ = only_expire_after;
}

void DumperOptions::set_resume_mode(bool resume_mode) {
  resume_mode_ = resume_mode;
}

void DumperOptions::set_is_s3_dump(bool is_s3_dump) {
  is_s3_dump_ = is_s3_dump;
}

void DumperOptions::set_s3_bucket_name(std::string s3_bucket) {
  s3_bucket_ = s3_bucket;
}

void DumperOptions::set_s3_final_path(std::string s3_path) {
  s3_path_ = s3_path;
}

void DumperOptions::set_sqs_queue_name(std::string sqs_queue) {
  sqs_queue_name_ = sqs_queue;
}

void DumperOptions::set_req_id(std::string req_id) {
  req_id_ = req_id;
}

void DumperOptions::add_dest_ip(const std::string& dest_ip) {
  dest_ips_.push_back(dest_ip);
}

void DumperOptions::add_all_ip(const std::string& all_ip) {
  all_ips_.push_back(all_ip);
}

void DumperOptions::set_ketama_bucket_size(uint32_t ketama_bucket_size) {
  ketama_bucket_size_ = ketama_bucket_size;
}

} // namespace memcachedumper
