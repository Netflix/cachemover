#include "common/logger.h"
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

#define BULK_GET_THRESHOLD 30
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
    need_drain_socket_(false) {
  mcdata_entries_pending_.reserve(BULK_GET_THRESHOLD);
}

void stupid_debug_func() {
  printf("Stupid debug func\n");
}

Status KeyValueWriter::Init() {
  rotating_data_files_.reset(new RotatingFile(data_file_prefix_, max_file_size_));
  RETURN_ON_ERROR(rotating_data_files_->Init());

  return Status::OK();
}

Status KeyValueWriter::WriteCompletedEntries() {

  MonotonicStopWatch total_msw;

  total_msw.Start();
  //uint32_t n_iovecs = num_complete_entries * PER_KEY_DATAPOINTS;
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
    //std::cout << it->first << " " << it->second->key() << " " << it->second->expiry() << std::endl;
    //std::cout << "Iovec writing KEY: " << mcdata_entry->key() << std::endl;
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

      /*std::cout << "(" << owning_thread_name_ <<  ", " << data_file_prefix_
                << ") IOVEC Writing elapsed: "
                << msw.ElapsedTime() << " | Nwritten: " << nwritten << std::endl;*/

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
  //std::cout << "GOING TO WRITE " << iovec_idx << " DATA!!!" << std::endl;

  //RotatingFile* out_files = new RotatingFile(data_file_prefix_, max_file_size_);
  //out_files->Init();

  //std::cout << "Wrote " << nwritten << " bytes." << std::endl << std::endl;

  //out_files->Finish();

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
  //std::cout << "Total WriteCompletedEntries elapsed: " << total_msw.ElapsedTime()
  //          << std::endl;

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
      std::cout << "Creaking coz no whitespace_after_key" << std::endl;
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    cur_key_ = std::string(value_delim_pos + 6, whitespace_after_key - value_delim_pos - 6);

    auto entry = mcdata_entries_processing_.find(cur_key_.c_str());
    if (entry == mcdata_entries_processing_.end()) {
      std::cout << "COULD NOT FIND KEY: " << cur_key_.c_str() << std::endl;
      assert(entry != mcdata_entries_processing_.end());
      abort();
    }

    const char* whitespace_after_flags = response_slice.next_whitespace();
    if (whitespace_after_flags == nullptr) {
      //stupid_debug_func();

      McData* mc = entry->second.get();
      // TODO: Handle partial buffer case.
      std::cout << "Creaking coz no whitespace_after_flags for key " << mc->key() << std::endl;
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    int32_t flags = std::stoi(std::string(whitespace_after_key + 1, whitespace_after_flags - whitespace_after_key - 1));

    const char* newline_after_datalen = response_slice.next_crlf();
    if (newline_after_datalen == nullptr) {
      //stupid_debug_func();

       std::cout << "KEY INFO TRUNCATED: " << cur_key_.c_str() << " | " << flags <<  std::endl;
      // TODO: Handle partial buffer case.
      //std::cout << "Creaking coz no newline_after_datalen" << std::endl << response_slice.data() << std::endl;
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    int32_t datalen = std::stoi(std::string(whitespace_after_flags + 1, newline_after_datalen - whitespace_after_flags - 1));

    McData* mcdata_entry = entry->second.get();
    mcdata_entry->setValueLength(datalen);


    //newline_after_data = response_slice.next_crlf();
    newline_after_data = response_slice.process_value(datalen);
    if (newline_after_data == nullptr) {
      // TODO: Handle partial buffer case.
      //std::cout << "Creaking coz no newline_after_DATA" << std::endl << response_slice.data() << " | Partial bytes to write: " << response_slice.bytes_pending() << std::endl;
      //std::cout << "Creaking coz no newline_after_DATA" << " | Partial bytes to write: " << response_slice.bytes_pending() << std::endl;
      broken_buffer_state_ = response_slice.parse_state();

      mcdata_entry->setValue(
          newline_after_datalen + 2, response_slice.bytes_pending());
    mcdata_entry->setValueLength(response_slice.bytes_pending());

      mcdata_entry->printValue();
      mcdata_entry->MarkComplete();
      ++n_complete_entries;
      break;
    }

    mcdata_entry->setValue(newline_after_datalen + 2, datalen);

    mcdata_entry->printValue();
    mcdata_entry->MarkComplete();
    ++n_complete_entries;

  } while (response_slice.bytes_pending() > 0); // TODO: Redundant loop condition; change.

  total_msw.Stop();
  //std::cout << "Total ProcessBulkResponse elapsed: " << total_msw.ElapsedTime()
  //          << std::endl;

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
    if (err == EAGAIN) {
      std::cout << owning_thread_name_ << ": Got EAGAIN. num_eagain_retries left: "
                << num_eagain_retries << std::endl;
      --num_eagain_retries;
    } else {
      break;
    }
  }

  if (err == EAGAIN) *broken_connection = true;
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
        &mcdata_entries_pending_, BULK_GET_THRESHOLD);

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

    /*std::cout << "(" << owning_thread_name_ <<  ", " << data_file_prefix_
          << ") Bulk get elapsed: "
          << msw.ElapsedTime() << "   ||  Free space: " << remaining_space
<< "  ||  nread: " << nread << std::endl;*/
    assert(nread > 0);
    buffer_current_ = buffer_current_ + nread;
    remaining_space = buffer_free_bytes();

    // Nothing to process.
    // TODO: Is this even possible? See assert(nread > 0) above.
    if (remaining_space == capacity_) {
      std::cout << "WTF???" << std::endl;
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
      std::cout << "BulkGetKeys failure. " << bulk_get_status.ToString() << std::endl;

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
      std::cout << "WriteCompletedEntries failure. "
                << write_status.ToString() << std::endl;
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
    std::cout << "Connection is broken, refreshing it." << std::endl;
    Status refresh_status = mc_sock_->Refresh();
    if (!refresh_status.ok()) {
      std::cout << "Failed to refresh connection" << std::endl;
      abort();
    }

    // We need to send another bulk get command since this is a new socket.
    need_drain_socket_ = false;
    // TODO: Avoid a recursive call.
    ProcessKeys(flush);
  }
  //std::cout << "Total time (ProcessKeys): " << total_msw.ElapsedTime()
  //          << "  |(" << owning_thread_name_
  //          << "): " << data_file_prefix_ << std::endl;
}

void KeyValueWriter::QueueForProcessing(McData* mc_key) {
  mcdata_entries_pending_.emplace(mc_key->key(), std::unique_ptr<McData>(mc_key));
  ++n_keys_pending_;
  ++total_keys_to_process_;

  //std::cout << "Added key: " << mc_key->key() << std::endl;
  // Time to do a bulk get of all keys gathered so far and write their values to a file.
  //if (mcdata_entries_.size() == BULK_GET_THRESHOLD) {
  if (n_keys_pending_ >= BULK_GET_THRESHOLD) {
    ProcessKeys(false);
  }
}

Status KeyValueWriter::Finalize() {
  ProcessKeys(true);

  // In case we couldn't flush a few keys.
  while (n_keys_pending_ > 0 || n_unwritten_processed_keys_ > 0) {
    ProcessKeys(true);
  }

  RETURN_ON_ERROR(rotating_data_files_->Finish());

  if (total_keys_to_process_ != num_processed_keys_) {
    std::cout << "MISMATCH! Finalized. Total keys given: " << total_keys_to_process_
            << " Total keys processed: " << num_processed_keys_ << std::endl;
  }
  return Status::OK();
}

void KeyValueWriter::PrintKeys() {

  std::cout << "PRINTING KEYS!!\n";

  McDataMap::iterator it = mcdata_entries_processing_.begin();

  while (it != mcdata_entries_processing_.end()) {

    std::cout << it->first << " " << it->second->key() << " " << it->second->expiry() << std::endl;
    it++;
  }
}

} // namespace memcachedumper
