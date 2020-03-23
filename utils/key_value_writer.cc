#include "common/logger.h"
#include "utils/file_util.h"
#include "utils/key_value_writer.h"
#include "utils/socket.h"

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

KeyValueWriter::KeyValueWriter(uint8_t* buffer, size_t capacity, Socket* mc_sock)
  : buffer_(buffer),
    capacity_(capacity),
    mc_sock_(mc_sock),
    last_buffer_partial_(false) {
  mcdata_entries_.reserve(BULK_GET_THRESHOLD);
}

void stupid_debug_func() {
  printf("Stupid debug func\n");
}

void KeyValueWriter::WriteCompletedEntries(uint32_t num_complete_entries) {

  //uint32_t n_iovecs = num_complete_entries * PER_KEY_DATAPOINTS;
  uint32_t n_iovecs = num_complete_entries * 2;
  struct iovec iovecs[n_iovecs];
  std::unordered_map<std::string, std::unique_ptr<McData>>::iterator it =
      mcdata_entries_.begin();

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
    std::cout << "Iovec writing KEY: " << mcdata_entry->key() << " | keylen: " << key_ref.length() << std::endl;
    it = mcdata_entries_.erase(it);
    iovec_idx += 2;

    stupid_debug_func();

  }
  std::cout << "GOING TO WRITE " << iovec_idx << " DATA!!!" << std::endl;

  int nwritten = writev(STDOUT_FILENO, iovecs, iovec_idx);
  std::cout << "Wrote " << nwritten << " bytes." << std::endl << std::endl;

  PosixFile *out_file = new PosixFile("TEST_FILE_UTIL");
  out_file->Open();

  out_file->Close();
  
}

bool KeyValueWriter::ProcessBulkResponse(uint8_t* buffer, int32_t bufsize) {

  if (last_buffer_partial_) {
    /*std::cout << "Last partial state: " << static_cast<int>(broken_buffer_state_) 
        << " | For key: " << cur_key_ << std::endl;*/
  }

  DataBufferSlice response_slice(reinterpret_cast<char*>(buffer), bufsize);
  bool reached_end = response_slice.reached_end();
  std::cout << "Reached end: " << reached_end << std::endl;
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
        std::cout << "Creaking coz no value_delim_pos" << std::endl;
        broken_buffer_state_ = response_slice.parse_state();
      }
      break;
    }

    const char* whitespace_after_key = response_slice.next_whitespace();
    if (whitespace_after_key == nullptr) {
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
      // TODO: Handle partial buffer case.
      std::cout << "Creaking coz no whitespace_after_flags" << std::endl;
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    int32_t flags = std::stoi(std::string(whitespace_after_key + 1, whitespace_after_flags - whitespace_after_key - 1));

    const char* newline_after_datalen = response_slice.next_crlf();
    if (newline_after_datalen == nullptr) {
      // TODO: Handle partial buffer case.
      std::cout << "Creaking coz no newline_after_datalen" << std::endl << response_slice.data() << std::endl;
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    int32_t datalen = std::stoi(std::string(whitespace_after_flags + 1, newline_after_datalen - whitespace_after_flags - 1));

    McData* mcdata_entry = entry->second.get();
    mcdata_entry->setValueLength(datalen);

    std::cout << "CUR KEY: " << cur_key_.c_str() << " | " << flags << " | " << datalen << std::endl;

    newline_after_data = response_slice.next_crlf();
    if (newline_after_data == nullptr) {
      // TODO: Handle partial buffer case.
      std::cout << "Creaking coz no newline_after_DATA" << std::endl << response_slice.data() << std::endl;
      broken_buffer_state_ = response_slice.parse_state();
      break;
    }

    mcdata_entry->setValue(newline_after_datalen + 1, datalen);

    mcdata_entry->printValue();
    mcdata_entry->MarkComplete();
    ++num_complete_entries;

  } while (newline_after_data != nullptr); // TODO: Redundant loop condition; change.

  WriteCompletedEntries(num_complete_entries);

  std::cout << "*-----------------*" << std::endl;
  return reached_end;
}

void KeyValueWriter::BulkGetKeys() {
  // Using untracked memory for the bulk get command should be fine. In the worst case,
  // we will use ~(BULK_GET_THRESHOLD * MAX_KEY_SIZE).
  std::stringstream bulk_get_cmd;

  // No keys to get.
  if (mcdata_entries_.empty()) {
    
    return;
  }

  bulk_get_cmd << "get ";
  std::unordered_map<std::string, std::unique_ptr<McData>>::iterator it =
      mcdata_entries_.begin();

  while (it != mcdata_entries_.end()) {
    bulk_get_cmd << it->first << " ";
    it++;
  }

  bulk_get_cmd << "\n";

  int32_t nsent;
  Status send_status = mc_sock_->Send(reinterpret_cast<const uint8_t*>(bulk_get_cmd.str().c_str()), bulk_get_cmd.str().length(), &nsent);
  if (!send_status.ok()) {
    LOG_ERROR("Could not send bulk get command");
  }


  std::cout << "Sent command [" << mcdata_entries_.size() << " keys]: " << bulk_get_cmd.str().c_str() << std::endl;
  {
    bool reached_end = false;
    while (!reached_end) {
      int32_t nread;
      Status recv_status = mc_sock_->Recv(buffer_, capacity_, &nread);
      if (!recv_status.ok()) {
        LOG_ERROR("Could not recv bulk get response.");
      }
      assert(nread > 0);

      // Make sure to bzero previously processed bytes so that we don't re-process
      // the same bytes again.
      if (static_cast<uint32_t>(nread) < capacity_) {
        bzero(const_cast<char*>(reinterpret_cast<char*>(buffer_) + nread), capacity_ - nread);
      }

      reached_end = ProcessBulkResponse(buffer_, nread);
    }
  }
  //std::cout << "BULK GETTING: " << std::endl << bulk_get_cmd.str() << std::endl;

  //mcdata_entries_.clear();
}

void KeyValueWriter::ProcessKey(McData* mc_key) {
  mcdata_entries_.emplace(mc_key->key(), std::unique_ptr<McData>(mc_key));

  std::cout << "Added key: " << mc_key->key() << std::endl;
  // Time to do a bulk get of all keys gathered so far and write their values to a file.
  if (mcdata_entries_.size() == BULK_GET_THRESHOLD) {
    BulkGetKeys();
  }
}

void KeyValueWriter::FlushPending() {
  std::cout << "Flushing keys" << std::endl;
  BulkGetKeys();
}

void KeyValueWriter::PrintKeys() {

  std::cout << "PRINTING KEYS!!\n";

  std::unordered_map<std::string, std::unique_ptr<McData>>::iterator it = mcdata_entries_.begin();

  while (it != mcdata_entries_.end()) {

    std::cout << it->first << " " << it->second->key() << " " << it->second->expiry() << std::endl;
    it++;
  }
}

} // namespace memcachedumper
