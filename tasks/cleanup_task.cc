/**
 *
 *  Copyright 2021 Netflix, Inc.
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

#include "tasks/cleanup_task.h"

#include "common/logger.h"
#include "tasks/process_metabuf_task.h"
#include "tasks/task_scheduler.h"
#include "tasks/task_thread.h"
#include "utils/mem_mgr.h"
#include "utils/metrics.h"
#include "utils/socket.h"

#include <string.h>

#include <string>
#include <fstream>
#include <memory>
#include <sstream>
#include <random>


#define METADUMP_END_STR "END\r\n"
#define METADUMP_BUSY_STR "BUSY currently processing crawler request\r\n"
#define METADUMP_END_STRLEN 5
#define METADUMP_BUSY_STRLEN 43

namespace memcachedumper {

//TODO: Clean up code.

CleanupTask::CleanupTask(MemoryManager *mem_mgr, std::string current_ip, bool enable_hash_table_crawl)
  : mem_mgr_(mem_mgr),
    current_ip_(current_ip),
    enable_hash_table_crawl_(enable_hash_table_crawl){
}

void CleanupTask::CleanupOrphanKeys(std::string chunk_keys) {
  size_t pos = chunk_keys.find("key=", 0);
  uint64_t total_keys = 0;
  uint64_t total_keys_deleted = 0;
  while(pos != std::string::npos) {
    std::string current_key = "";
    int end_idx = chunk_keys.find(" ", pos+1);
    current_key = chunk_keys.substr(pos+4, end_idx - pos - 4);
    total_keys++;
    if (MemcachedUtils::CheckIfKeyIsOrphan(current_key, current_ip_)) {
      std::string delete_cmd = "delete " + current_key + "\n";
      Status send_status = SendCommand(delete_cmd);
      if (send_status.ok()) {
        total_keys_deleted++;
      }
    }
    pos = chunk_keys.find("key=", pos+1);
  }

  DumpMetrics::increment_total_metadump_keys(total_keys);
  DumpMetrics::increment_total_keys_cleaned(total_keys_deleted);
}

void CleanupTask::Execute() {
  memcached_socket_ = owning_thread()->task_scheduler()->GetMemcachedSocket();
  assert(memcached_socket_ != nullptr);

  bool busy_crawler = true;
  std::random_device rand_device;
  std::mt19937 rand_generator(rand_device());
  std::uniform_int_distribution<> uniform_distribution(3, 19);

  while (busy_crawler) {

    busy_crawler = false;
    std::string metadump_cmd("lru_crawler metadump all\n");
    if (enable_hash_table_crawl_) {
        metadump_cmd = "lru_crawler metadump hash\n";
    }
    Status send_status = SendCommand(metadump_cmd);
    if (!send_status.ok()) {
      LOG_ERROR("SendCommand() failed. (Status: {0})", send_status.ToString());
      abort();
    }

    Status stat = RecvResponse();

    if (stat.IsBusyLRUCrawler()) {
      int sleep_duration_s = uniform_distribution(rand_generator);
      LOG_ERROR("LRU crawler is busy. Retrying after {0} seconds.", sleep_duration_s);
      sleep(sleep_duration_s);
      busy_crawler = true;
      continue;
    } else if (stat.IsEOFError()) {
      // This is a temporary way to deal with EAGAIN.
      // TODO: This was done as a quick patch. Clean up later.
      int sleep_duration_s = uniform_distribution(rand_generator);
      LOG_ERROR("Memcached not responding. Retrying after {0} seconds.", sleep_duration_s);
      sleep(sleep_duration_s);
      // Treat as busy crawler for now.
      busy_crawler = true;
    } else if (!stat.ok()) {
      LOG_ERROR("RecvResponse() failed. (Status: {0})", stat.ToString());
      assert(false);
    }

  }

  // Return socket.
  owning_thread()->task_scheduler()->ReleaseMemcachedSocket(memcached_socket_);
  memcached_socket_ = nullptr;
}


// TODO: Move into utils
Status CleanupTask::SendCommand(const std::string& cleanup_cmd) {
  int32_t bytes_sent;

  RETURN_ON_ERROR(memcached_socket_->Send(
      reinterpret_cast<const uint8_t*>(cleanup_cmd.c_str()),
      cleanup_cmd.length(), &bytes_sent));

  return Status::OK();
}

Status CleanupTask::RecvResponse() {

  LOG("Beginning to receive key dump...");
  uint8_t *buf = mem_mgr_->GetBuffer();
  assert(buf != nullptr);
  uint64_t chunk_size = mem_mgr_->chunk_size();
  int32_t bytes_read = 0;

  bool reached_end = false;
  bool busy_crawler = false;

  std::string previous_buffer_pending = "";

  do {
    Status stat = memcached_socket_->Recv(buf, chunk_size-1, &bytes_read, 2, true);
    if (!stat.ok() && !stat.IsNetworkError() && !stat.IsEOFError()) {
      mem_mgr_->ReturnBuffer(buf);
      return stat;
    }

    if (stat.IsEOFError()) {
      reached_end = true;
      if (previous_buffer_pending.length()>0) {
        CleanupOrphanKeys(previous_buffer_pending);
        previous_buffer_pending = "";
      }
      mem_mgr_->ReturnBuffer(buf);
      return Status::OK();
    }

    uint8_t *unwritten_tail = nullptr;
    busy_crawler = (strncmp(reinterpret_cast<const char*>(&buf[bytes_read - METADUMP_BUSY_STRLEN]),
            METADUMP_BUSY_STR, METADUMP_BUSY_STRLEN) == 0) ? true:false;


    if (busy_crawler) {
      LOG("lru crawler busy, will retry");
      mem_mgr_->ReturnBuffer(buf);
      return Status::BusyLRUCrawler("LRU crawler is busy");
    }

    std::string chunk_string(reinterpret_cast<char*>(buf), bytes_read);
    chunk_string = previous_buffer_pending + chunk_string;
    // Iterate over all the keys fully present in this chunk
    size_t last_key_pos = chunk_string.rfind("key=", bytes_read);
    if (last_key_pos == std::string::npos) {
      mem_mgr_->ReturnBuffer(buf);
      return Status::OK();
    }

    std::string chunk_keys(reinterpret_cast<char*>(buf), last_key_pos);
    CleanupOrphanKeys(chunk_keys);

    // Save the position from the last "key=" onwards to write to the next file.
    unwritten_tail = buf + last_key_pos;

    // Write the last partial line if it exists.
    if (unwritten_tail) {
        size_t remaining_bytes = bytes_read - last_key_pos;
        std::string tail_string(reinterpret_cast<char*>(unwritten_tail), remaining_bytes);
        previous_buffer_pending = tail_string;
      }

  } while (bytes_read != 0 && !reached_end);

  mem_mgr_->ReturnBuffer(buf);

  return Status::OK();
}

} // namespace memcachedumper
