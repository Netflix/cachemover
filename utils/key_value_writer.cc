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

#include "common/logger.h"
#include "utils/aws_utils.h"
#include "utils/file_util.h"
#include "utils/key_value_writer.h"
#include "utils/socket.h"
#include "utils/stopwatch.h"

#include <stdio.h>
#include <sys/uio.h>
#include <unistd.h>

#include <iostream>
#include <sstream>
#include <string>

#define MC_VALUE_DELIM "VALUE "

#define INJECT_EAGAIN_EVERY_N 0

#if INJECT_EAGAIN_EVERY_N
static int32_t inject_every_n = INJECT_EAGAIN_EVERY_N;
#endif

// We write 5 datapoints per key (in UTF-8):
// <key> <expiry> <flags> <datalen> <data>
#define PER_KEY_DATAPOINTS 5

namespace memcachedumper {

KeyValueWriter::KeyValueWriter(std::string data_file_prefix,
    std::string owning_thread_name, uint8_t* buffer,
    size_t capacity, uint64_t max_file_size, Socket* mc_sock)
  : data_file_prefix_(data_file_prefix),
    owning_thread_name_(owning_thread_name),
    buffer_begin_(buffer),
    buffer_current_(buffer),
    process_from_(buffer),
    capacity_(capacity),
    max_file_size_(max_file_size),
    mc_sock_(mc_sock),
    n_keys_pending_(0),
    n_unwritten_processed_keys_(0),
    total_keys_to_process_(0),
    num_processed_keys_(0),
    num_missing_keys_(0),
    need_drain_socket_(false) {
  mcdata_entries_pending_.reserve(MemcachedUtils::BulkGetThreshold());
}

void stupid_debug_func() {
  printf("Stupid debug func\n");
}

Status KeyValueWriter::Init() {
  rotating_data_files_.reset(
      new RotatingFile(
        MemcachedUtils::GetDataStagingPath(),
        data_file_prefix_,
        max_file_size_,
        MemcachedUtils::GetDataFinalPath(),
        true /* suffix checksum */,
	AwsUtils::GetS3Bucket().empty() ? false : true /* Upload each file to S3 on close */));
  RETURN_ON_ERROR(rotating_data_files_->Init());
  return Status::OK();
}

Status KeyValueWriter::WriteCompletedEntries() {

  MonotonicStopWatch total_msw;

  total_msw.Start();
  uint32_t n_iovecs = std::min(
      static_cast<uint32_t>(1024), n_unwritten_processed_keys_ * 2);
  struct iovec iovecs[n_iovecs];

  // We allocate memory on the stack to hold the key metadata strings until
  // we've written it our through WriteV().
  std::string key_mds[n_iovecs / 2];

  McDataMap::iterator it = mcdata_entries_processing_.begin();

  uint32_t iovec_idx = 0;

  while (it != mcdata_entries_processing_.end()) {

    McData* mcdata_entry = it->second.get();

    if (!mcdata_entry->Complete()) {
      ++it;
      continue;
    }

    int key_mds_idx = iovec_idx / 2;
    key_mds[key_mds_idx] = MemcachedUtils::CraftMetadataString(mcdata_entry);
    iovecs[iovec_idx].iov_base = const_cast<char*>(key_mds[key_mds_idx].c_str());
    iovecs[iovec_idx].iov_len = key_mds[key_mds_idx].length();

    iovecs[iovec_idx + 1].iov_base = mcdata_entry->Value();
    iovecs[iovec_idx + 1].iov_len = mcdata_entry->ValueLength();
    ++num_processed_keys_;
    ++it;
    iovec_idx += 2;

    if (iovec_idx == n_iovecs) {
      ssize_t nwritten = 0;
      MonotonicStopWatch msw;

      {
        SCOPED_STOP_WATCH(&msw);
        RETURN_ON_ERROR(rotating_data_files_->WriteV(iovecs, iovec_idx, &nwritten));
      }

      iovec_idx = 0;
      n_unwritten_processed_keys_ -= (n_iovecs / 2);
    }
  }

  // Flush remaining entries.
  if (iovec_idx > 0) {
    ssize_t nwritten = 0;
    RETURN_ON_ERROR(rotating_data_files_->WriteV(iovecs, iovec_idx, &nwritten));
    n_unwritten_processed_keys_ -= (iovec_idx / 2);
  }

  // TODO: Find better way to erase written entries from map.
  it = mcdata_entries_processing_.begin();
  while (it != mcdata_entries_processing_.end()) {

    McData* mcdata_entry = it->second.get();

    if (!mcdata_entry->Complete()) {
      ++it;
      continue;
    }
    it = mcdata_entries_processing_.erase(it);
  }

  total_msw.Stop();

  return Status::OK();
}

uint32_t KeyValueWriter::ProcessBulkResponse() {

  // Buffer is empty, nothing to process.
  if (buffer_free_bytes() == capacity_) return 0;

  MonotonicStopWatch total_msw;
  total_msw.Start();
  // Wrap the buffer in a DataBufferSlice for easier processing.
  DataBufferSlice response_slice(reinterpret_cast<char*>(process_from_),
      buffer_current_ - process_from_);

  uint32_t n_complete_entries = 0;
  const char* newline_after_data = nullptr;
  do {

    const char* value_delim_pos = response_slice.next_value_delim();
    if (value_delim_pos == nullptr) {
      break;
    }

    const char* whitespace_after_key = response_slice.next_whitespace();
    if (whitespace_after_key == nullptr) {
      //stupid_debug_func();

      // TODO: Handle partial buffer case.
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    cur_key_ = std::string(value_delim_pos + 6, whitespace_after_key - value_delim_pos - 6);

    auto entry = mcdata_entries_processing_.find(cur_key_.c_str());
    if (entry == mcdata_entries_processing_.end()) {
      LOG_ERROR("COULD NOT FIND KEY: {0}", cur_key_.c_str());
      assert(entry != mcdata_entries_processing_.end());
      abort();
    }

    const char* whitespace_after_flags = response_slice.next_whitespace();
    if (whitespace_after_flags == nullptr) {
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    McData* mcdata_entry = entry->second.get();
    uint16_t flags = std::stoul(
        std::string(whitespace_after_key + 1,
            whitespace_after_flags - whitespace_after_key - 1));
    mcdata_entry->setFlags(flags);

    const char* newline_after_datalen = response_slice.next_crlf();
    if (newline_after_datalen == nullptr) {
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    int32_t datalen = std::stoi(
        std::string(whitespace_after_flags + 1,
            newline_after_datalen - whitespace_after_flags - 1));
    mcdata_entry->setValueLength(datalen);

    newline_after_data = response_slice.process_value(datalen);
    if (newline_after_data == nullptr) {
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    mcdata_entry->setValue(newline_after_datalen + 2, datalen);

    mcdata_entry->MarkComplete();
    ++n_complete_entries;

  } while (response_slice.bytes_pending() > 0); // TODO: Redundant loop condition; change.

  total_msw.Stop();

  return n_complete_entries;
}

Status KeyValueWriter::RecvFromMemcached(uint8_t *buf, int32_t size, int32_t *nread,
    bool* broken_connection) {
  Status recv_status = Status::OK();
  int err = 0;
  int num_eagain_retries = 3;

#if INJECT_EAGAIN_EVERY_N
  // Spoof an EAGAIN for testing.
  if (--inject_every_n == 0) {
    inject_every_n = INJECT_EAGAIN_EVERY_N;
    *broken_connection = true;
    return Status::NetworkError("Injected EAGAIN", "");
  }
#endif
  while (num_eagain_retries > 0) {
    recv_status = mc_sock_->Recv(buf, size, nread);
    if (recv_status.ok()) { return Status::OK(); }
    err = errno;
    if (err == EAGAIN || err == ESHUTDOWN) {
      LOG_ERROR("Got EAGAIN. Recv retries left: {0}", num_eagain_retries);
      --num_eagain_retries;
    } else {
      break;
    }
  }

  if (err == EAGAIN || err == ESHUTDOWN) *broken_connection = true;
  return recv_status;
}

void KeyValueWriter::PromoteKeysFromPending() {
  McDataMap::iterator it = mcdata_entries_pending_.begin();

  while (it != mcdata_entries_pending_.end()) {
    if (!it->second->get_complete()) {
      // Move these keys from pending map to processing map.
      McDataMap::iterator entry_in_processing = mcdata_entries_processing_.emplace(
          it->first, it->second.release()).first;
      entry_in_processing->second->set_get_complete(true);
      it = mcdata_entries_pending_.erase(it);

      --n_keys_pending_;
    } else {
      ++it;
    }
  }
}

void KeyValueWriter::DemoteKeysToPending() {
  McDataMap::iterator it = mcdata_entries_processing_.begin();

  while (it != mcdata_entries_processing_.end()) {
    if (!it->second->Complete()) {
      // If we think it's evicted, delete the key, else move it back to the pending
      // map.
      if (it->second->PossiblyEvicted()) {
        ++num_missing_keys_;
      } else {
        ++n_keys_pending_;
        McDataMap::iterator entry_in_pending = mcdata_entries_pending_.emplace(
            it->first, it->second.release()).first;
        entry_in_pending->second->set_get_complete(false);
      }
      it = mcdata_entries_processing_.erase(it);

    } else {
      ++it;
    }
  }
}

Status KeyValueWriter::BulkGetKeys(bool* broken_connection) {

  // Skip sending a command if we're yet to finish processing the response to a
  // previous command.
  if (need_drain_socket_ == false) {

    // Craft a bulk get command with all the pending keys.
    std::string bulk_get_cmd = MemcachedUtils::CraftBulkGetCommand(
        &mcdata_entries_pending_);

    if (bulk_get_cmd.empty()) return Status::OK();

    Status send_status;
    int32_t unused;
    RETURN_ON_ERROR(mc_sock_->Send(
        reinterpret_cast<const uint8_t*>(bulk_get_cmd.c_str()),
        bulk_get_cmd.length(), &unused));
  }

  bool reached_end = false;
  do {
    int32_t nread = 0;
    MonotonicStopWatch msw;
    size_t remaining_space = buffer_free_bytes();
    {
      SCOPED_STOP_WATCH(&msw);
      RETURN_ON_ERROR(RecvFromMemcached(buffer_current_, remaining_space, &nread,
          broken_connection));
    }

    assert(nread > 0);
    buffer_current_ = buffer_current_ + nread;
    remaining_space = buffer_free_bytes();

    // Nothing to process.
    // TODO: Is this even possible? See assert(nread > 0) above.
    if (remaining_space == capacity_) {
      LOG("WTF???");
      return Status::OK();
    }

    // Return here since there's more free space in the buffer that we can fill up
    // before processing the buffer; unless we've been instructed to flush.
    reached_end = !strncmp(const_cast<char*>(
        reinterpret_cast<char*>(buffer_current_)) - 5, "END\r\n", 5);

    if (remaining_space == 0) {
      if (!reached_end) need_drain_socket_ = true;
      return Status::OK();
    }

  } while (!reached_end);

  // Override the "END\r\n" delimiter if we're going to fill this buffer some
  // more before sending it up for processing.
  buffer_current_ -= 5;
  need_drain_socket_ = false;

  return Status::OK();
}

void KeyValueWriter::ProcessKeys(bool flush) {
  MonotonicStopWatch total_msw;
  SCOPED_STOP_WATCH(&total_msw);
  // No keys to get.
  if (mcdata_entries_pending_.empty() && mcdata_entries_processing_.empty()) {
    return;
  }

  bool broken_connection = false;
  bool did_write = false;
  do {
    Status bulk_get_status = BulkGetKeys(&broken_connection);
    if (!bulk_get_status.ok() && !broken_connection) {
      LOG_ERROR("BulkGetKeys failure. (Status: {0})", bulk_get_status.ToString());

      // TODO: Fail gracefully.
      abort();
    }

    // Since we sent the bulk get command and started receiving values, mark those
    // keys as processing from pending.
    PromoteKeysFromPending();

    // If we're not asked to flush and the buffer is not full, postpone writing them
    // for later.
    if (!broken_connection && !flush && buffer_free_bytes() > 0) break;

    // Process the buffer and make the McData entries ready for writing.
    n_unwritten_processed_keys_ += ProcessBulkResponse();

    // Write fully processed entries.
    Status write_status = WriteCompletedEntries();
    if (!write_status.ok()) {
      LOG_ERROR("WriteCompletedEntries failure. (Status: {0})", write_status.ToString());
      // TODO: Fail gracefully.
      abort();
    }
    did_write = true;

    // Reset the buffer.
    buffer_current_ = buffer_begin_;
    process_from_ = buffer_begin_;

    if (broken_connection) break;
  // If we're yet to complete draining the socket, go back and complete the cycle.
  } while (need_drain_socket_ == true);

  // All the keys we attempted to process but still haven't are marked as pending
  // again, so that we can get and write them the next time this function is called.
  if (did_write) {
    // Process one last time since some keys might have been returned in the last call
    // to BulkGetKeys(), and we don't want to demote those; else we will end up
    // asking memcached for the same key(s) twice.
    n_unwritten_processed_keys_ += ProcessBulkResponse();

    // Next time we call ProcessBulkResponse(), start processing from here, since the
    // data before has already been processed.
    process_from_ = buffer_current_;

    DemoteKeysToPending();
  }

  // If our connection was broken, refresh the connection.
  // We will process the keys in the next call to this function.
  if (broken_connection) {
    LOG("Connection is broken, refreshing it.");
    Status refresh_status = mc_sock_->Refresh();
    if (!refresh_status.ok()) {
      LOG_ERROR("Failed to refresh connection");
      abort();
    }

    // We need to send another bulk get command since this is a new socket.
    need_drain_socket_ = false;
    // TODO: Avoid a recursive call.
    ProcessKeys(flush);
  }
}

void KeyValueWriter::QueueForProcessing(McData* mc_key) {
  mcdata_entries_pending_.emplace(mc_key->key(), std::unique_ptr<McData>(mc_key));
  ++n_keys_pending_;
  ++total_keys_to_process_;

  // Time to do a bulk get of all keys gathered so far and write their values to a file.
  //if (mcdata_entries_.size() == BULK_GET_THRESHOLD) {
  if (n_keys_pending_ >= MemcachedUtils::BulkGetThreshold()) {
    ProcessKeys(false);
  }
}

Status KeyValueWriter::Finalize() {
  ProcessKeys(true);

  // In case we couldn't flush a few keys.
  while (n_keys_pending_ > 0 || n_unwritten_processed_keys_ > 0) {
    // We noticed this happen very rarely. This assert should result in a core
    // file which we can use to track the state of the process when it happened
    // and fix the potential bug.
    assert(!(mcdata_entries_pending_.empty() && mcdata_entries_processing_.empty()));
    ProcessKeys(true);
  }

  RETURN_ON_ERROR(rotating_data_files_->Finish());

  if (total_keys_to_process_ != num_processed_keys_) {
    LOG("MISMATCH! Finalized. Total keys given: {0} Total keys processed + missing: {1}",
        total_keys_to_process_, num_processed_keys_ + num_missing_keys_);
  }
  return Status::OK();
}

void KeyValueWriter::PrintKeys() {

  LOG("[DEBUG] PRINTING KEYS!!");

  McDataMap::iterator it = mcdata_entries_processing_.begin();

  while (it != mcdata_entries_processing_.end()) {

    LOG("{0} {1} {2}", it->first, it->second->key(), it->second->expiry());
    it++;
  }
}

} // namespace memcachedumper
