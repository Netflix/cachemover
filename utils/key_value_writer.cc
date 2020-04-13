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
    capacity_(capacity),
    max_file_size_(max_file_size),
    mc_sock_(mc_sock),
    n_keys_pending_(0),
    last_buffer_partial_(false) {
  mcdata_entries_.reserve(BULK_GET_THRESHOLD);
}

void stupid_debug_func() {
  printf("Stupid debug func\n");
}

Status KeyValueWriter::Init() {
  rotating_data_files_.reset(new RotatingFile(data_file_prefix_, max_file_size_));
  RETURN_ON_ERROR(rotating_data_files_->Init());

  return Status::OK();
}

void KeyValueWriter::WriteCompletedEntries(uint32_t num_complete_entries) {

  //uint32_t n_iovecs = num_complete_entries * PER_KEY_DATAPOINTS;
  uint32_t n_iovecs = num_complete_entries * 2;
  struct iovec iovecs[n_iovecs];
  McDataMap::iterator it = mcdata_entries_.begin();

  uint32_t iovec_idx = 0;

  while (it != mcdata_entries_.end()) {

    McData* mcdata_entry = it->second.get();

    if (!mcdata_entry->Complete()) {
      ++it;
      continue;
    }

    std::string& key_ref = mcdata_entry->key();
    std::string key_raw(key_ref);
    iovecs[iovec_idx].iov_base = const_cast<char*>(key_ref.c_str());
    iovecs[iovec_idx].iov_len = key_ref.length();

    iovecs[iovec_idx + 1].iov_base = mcdata_entry->Value();
    iovecs[iovec_idx + 1].iov_len = mcdata_entry->ValueLength();
    //std::cout << it->first << " " << it->second->key() << " " << it->second->expiry() << std::endl;
    //std::cout << "Iovec writing KEY: " << mcdata_entry->key() << std::endl;
    ++num_processed_keys_;
    ++it;
    iovec_idx += 2;

    if (iovec_idx == 1024) {
      ssize_t nwritten = 0;
      MonotonicStopWatch msw;

      std::cout << "iovec_idx: " << iovec_idx << std::endl;
      {
        SCOPED_STOP_WATCH(&msw);
        Status write_status = rotating_data_files_->WriteV(iovecs, iovec_idx, &nwritten);
        if (!write_status.ok()) {
          std::cout << "ERROR: Could not write to file: " << write_status.ToString() << std::endl;
        }
      }

      std::cout << "(" << owning_thread_name_ <<  ", " << data_file_prefix_
                << ") IOVEC Writing elapsed: "
                << msw.ElapsedTime() << " | Nwritten: " << nwritten << std::endl;

      iovec_idx = 0;
    }
  }
  //std::cout << "GOING TO WRITE " << iovec_idx << " DATA!!!" << std::endl;

  //RotatingFile* out_files = new RotatingFile(data_file_prefix_, max_file_size_);
  //out_files->Init();

  //std::cout << "Wrote " << nwritten << " bytes." << std::endl << std::endl;

  //out_files->Finish();

  // TODO: Find better way to erase written entries from map.
  it = mcdata_entries_.begin();
  while (it != mcdata_entries_.end()) {

    McData* mcdata_entry = it->second.get();

    if (!mcdata_entry->Complete()) {
      ++it;
      continue;
    }
    //std::cout << "Iovec ERASING KEY: " << mcdata_entry->key() << std::endl;
    it = mcdata_entries_.erase(it);
  }
}

bool KeyValueWriter::ProcessBulkResponse(uint8_t* buffer, int32_t bufsize) {

  if (last_buffer_partial_) {
    /*std::cout << "Last partial state: " << static_cast<int>(broken_buffer_state_) 
        << " | For key: " << cur_key_ << std::endl;*/
  }

  //printf("Buffer: %.*s", 1024, buffer);

  DataBufferSlice response_slice(reinterpret_cast<char*>(buffer), bufsize);
  bool reached_end = response_slice.reached_end();
  //std::cout << "Reached end: " << reached_end << std::endl;
  if (!reached_end) {
    last_buffer_partial_ = true;
    //std::cout << "Partial buffer: " << response_slice.data() << std::endl;
  }

  uint32_t num_complete_entries = 0;
  const char* newline_after_data = nullptr;
  do {

    const char* value_delim_pos = response_slice.next_value_delim();
    if (value_delim_pos == nullptr) {
      // TODO: Handle partial buffer case.
      if (!reached_end) {
        //stupid_debug_func();
        std::cout << "Creaking coz no value_delim_pos" << std::endl;
        broken_buffer_state_ = response_slice.parse_state();
      }
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

    auto entry = mcdata_entries_.find(cur_key_.c_str());
    assert(entry != mcdata_entries_.end());
    if (entry == mcdata_entries_.end()) {
      std::cout << "COULD NOT FIND KEY: " << cur_key_.c_str() << std::endl;
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

      // TODO: Handle partial buffer case.
      std::cout << "Creaking coz no newline_after_datalen" << std::endl << response_slice.data() << std::endl;
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    int32_t datalen = std::stoi(std::string(whitespace_after_flags + 1, newline_after_datalen - whitespace_after_flags - 1));

    McData* mcdata_entry = entry->second.get();
    mcdata_entry->setValueLength(datalen);

    //std::cout << "CUR KEY: " << cur_key_.c_str() << " | " << flags << " | " << datalen << std::endl;

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
      ++num_complete_entries;
      break;
    }

    mcdata_entry->setValue(newline_after_datalen + 2, datalen);

    mcdata_entry->printValue();
    mcdata_entry->MarkComplete();
    ++num_complete_entries;

  } while (newline_after_data != nullptr); // TODO: Redundant loop condition; change.

  WriteCompletedEntries(num_complete_entries);

  //std::cout << "*-----------------*" << std::endl;
  return reached_end;
}

void KeyValueWriter::BulkGetKeys(bool flush) {
  // Using untracked memory for the bulk get command should be fine. In the worst case,
  // we will use ~(BULK_GET_THRESHOLD * MAX_KEY_SIZE).
  std::stringstream bulk_get_cmd;

  MonotonicStopWatch total_msw;

  {
    SCOPED_STOP_WATCH(&total_msw);
    // No keys to get.
    if (mcdata_entries_.empty()) {
      return;
    }


    uint8_t num_keys_to_get = 0;
    std::vector<std::string> keys_processing;
    {
      bulk_get_cmd << "get ";
      McDataMap::iterator it = mcdata_entries_.begin();
      while (it != mcdata_entries_.end()) {
        if (!it->second->get_complete()) {
          keys_processing.push_back(it->first);
          bulk_get_cmd << it->first << " ";
          ++num_keys_to_get;
        }
        it++;
      }
    }

    bulk_get_cmd << "\n";

    int32_t nsent;

    if (num_keys_to_get > 0) {
      Status send_status;
    //std::cout << "Sending command [" << mcdata_entries_.size() << " keys]: " << bulk_get_cmd.str().c_str() << std::endl;
      send_status = mc_sock_->Send(
          reinterpret_cast<const uint8_t*>(bulk_get_cmd.str().c_str()),
          bulk_get_cmd.str().length(), &nsent);
      if (!send_status.ok()) {
        LOG_ERROR("Could not send bulk get command");
      }

    }

    {
      Status recv_status;
      bool reached_end = false;
      while (!reached_end) {
        int32_t nread = 0;
        MonotonicStopWatch msw;
        size_t remaining_space = (buffer_begin_ + capacity_) - buffer_current_;
        if (num_keys_to_get > 0) {
          {
            SCOPED_STOP_WATCH(&msw);
            recv_status = mc_sock_->Recv(buffer_current_, remaining_space, &nread);
          }
          std::cout << "(" << owning_thread_name_ <<  ", " << data_file_prefix_
                << ") Bulk get elapsed: "
                << msw.ElapsedTime() << std::endl;
          if (!recv_status.ok()) {
            LOG_ERROR("Could not recv bulk get response.");
            std::cout << "Could not recv bulk get response." << recv_status.ToString() << std::endl;
            break;
          }
          assert(nread > 0);
        }
        buffer_current_ = buffer_current_ + nread;
        remaining_space = (buffer_begin_ + capacity_) - buffer_current_;

        // Nothing to process.
        // TODO: Make this condition more clean.
        if (remaining_space == capacity_) break;

        for (std::vector<std::string>::iterator vit = keys_processing.begin();
          vit != keys_processing.end();) {
          //std::cout << "Marking GET complete for: " << *vit << std::endl;
          mcdata_entries_[*vit]->set_get_complete(true);
          --n_keys_pending_;
          vit = keys_processing.erase(vit);
        }

        // Make sure to bzero previously processed bytes so that we don't re-process
        // the same bytes again.
        if (remaining_space > 0) {
          // TODO: This is very expensive. Find alternate way.
          bzero(const_cast<char*>(reinterpret_cast<char*>(buffer_current_)),
              remaining_space);

          //stupid_debug_func();
          // Return here since there's more free space in the buffer that we can fill up
          // before processing the buffer; unless we've been instructed to flush.
          if (!flush) {
            //std::cout << "No flush, hence breaking due to free space of: " << remaining_space << std::endl;
            // Override the "END\r\n" delimiter if we're going to fill this buffer some
            // more before sending it up for processing.
            buffer_current_ -= 5;
            return;
          }
        }

        // Now that the buffer is full (or 'flush' == true), process it entirely.
        reached_end = ProcessBulkResponse(buffer_begin_, capacity_ - remaining_space);

        // Reset the buffer.
        buffer_current_ = buffer_begin_;

      }

      McDataMap::iterator it = mcdata_entries_.begin();
      while (it != mcdata_entries_.end()) {

        McData* mcdata_entry = it->second.get();

        if (!mcdata_entry->Complete()) {
          //std::cout << "UNMarking GET complete: " << mcdata_entry->key() << std::endl;
          mcdata_entry->set_get_complete(false);
          ++n_keys_pending_;
          ++it;
          continue;
        } else {
          std::cout << "WARNING: Completed entry still in map" << std::endl;
        }
      }

    }
    std::cout << "Total time (BulkGetKeys): " << total_msw.ElapsedTime() << std::endl;
  }
  //std::cout << "BULK GETTING: " << std::endl << bulk_get_cmd.str() << std::endl;

  //mcdata_entries_.clear();
}

void KeyValueWriter::ProcessKey(McData* mc_key) {
  mcdata_entries_.emplace(mc_key->key(), std::unique_ptr<McData>(mc_key));
  ++n_keys_pending_;

  //std::cout << "Added key: " << mc_key->key() << std::endl;
  // Time to do a bulk get of all keys gathered so far and write their values to a file.
  //if (mcdata_entries_.size() == BULK_GET_THRESHOLD) {
  if (n_keys_pending_ == BULK_GET_THRESHOLD) {
    BulkGetKeys(false);
  }
}

Status KeyValueWriter::Finalize() {
  std::cout << "Flushing keys" << std::endl;
  BulkGetKeys(true);

  RETURN_ON_ERROR(rotating_data_files_->Finish());

  return Status::OK();
}

void KeyValueWriter::PrintKeys() {

  std::cout << "PRINTING KEYS!!\n";

  McDataMap::iterator it = mcdata_entries_.begin();

  while (it != mcdata_entries_.end()) {

    std::cout << it->first << " " << it->second->key() << " " << it->second->expiry() << std::endl;
    it++;
  }
}

} // namespace memcachedumper
