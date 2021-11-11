// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "utils/file_util.h"
#include "utils/memcache_utils.h"

#include <string>
#include <unordered_map>

namespace memcachedumper {

// Forward declarations.
class Socket;

class KeyValueWriter {
 public:
  KeyValueWriter(std::string data_file_prefix, std::string owning_thread_name,
      uint8_t* buffer, size_t capacity, uint64_t max_file_size, Socket* mc_sock);

  // Initialize the KeyValueWriter.
  Status Init();

  // Tears down any state and flushes pending keys for bulk get and writing, if any,
  // from the mcdata_entries_.
  Status Finalize();

  // Adds 'mc_key' to the entries to get the value for and write to a file.
  void QueueForProcessing(McData* mc_key);

  void PrintKeys();

  uint64_t num_processed_keys() { return num_processed_keys_; }
  uint64_t num_missing_keys() { return num_missing_keys_; }

 private:

  // Get values from Memcached for keys present in mcdata_entries_ map.
  // If 'flush' is true, it will get all the keys from mcdata_entries_ even if
  // our buffer is not used to 'capacity_'.
  void ProcessKeys(bool flush);

  // Process the response from Memcached and populate mcdata_entries_processing_
  // appropriately.
  uint32_t ProcessBulkResponse();

  // Writes entires marked as complete from 'mcdata_entries_' to the final output
  // file.
  Status WriteCompletedEntries();

  Status BulkGetKeys(bool* broken_connection);

  // Helper function to Recv() data from memcached with retry loops and connection
  // refreshes on failures.
  Status RecvFromMemcached(uint8_t *buf, int32_t size, int32_t *nread,
      bool* broken_connection);

  // Move keys from mcdata_entries_pending_ map to mcdata_entries_processing_ map.
  // Must only be called if we have already sent a get command to MC for these keys.
  void PromoteKeysFromPending();

  // Move keys that we attempted to process but did not complete back to the
  // mcdata_entries_pending_ map.
  void DemoteKeysToPending();

  inline uint64_t buffer_free_bytes() {
    return buffer_begin_ + capacity_ - buffer_current_;
  }

  // The prefix to use for every file created by this object.
  std::string data_file_prefix_;
  // Name of the thread that's operating on this object.
  const std::string owning_thread_name_;
  // The address of the buffer to use while getting values from Memcached.
  uint8_t* buffer_begin_;
  // A pointer to the first free byte in buffer_begin_.
  uint8_t* buffer_current_;
  // Pointer in buffer until where we've processed so far. If nothing is processed,
  // it pointes to 'buffer_begin_'.
  uint8_t* process_from_;
  // The size of 'buffer_'.
  size_t capacity_;
  // Maximum size of file to create.
  uint64_t max_file_size_;
  // Socket to talk to Memcached.
  Socket* mc_sock_;
  // Map of key name to McData entries that we have yet to get the values for.
  //std::unordered_map<std::string, std::unique_ptr<McData>> mcdata_entries_;
  McDataMap mcdata_entries_pending_;

  // Map of keys to McData entries for which we have already called bulk get.
  // Once a key from this map is written to disk, it will be erased from this map.
  McDataMap mcdata_entries_processing_;

  // Number of keys waiting to get data for.
  uint32_t n_keys_pending_;

  // The number of keys we've processed (i.e. populated the corressponding McData with
  // data and flags), but not yet written.
  uint32_t n_unwritten_processed_keys_;

  // Total number of keys we've been asked to process.
  uint32_t total_keys_to_process_;

  // Number of keys we've successfully processed.
  uint64_t num_processed_keys_;

  // Number of keys we've tried to repeatedly call "get" on but received no data
  // back for.
  uint64_t num_missing_keys_;

  // While parsing responses from Memcached, this keeps track of the current key
  // being processed.
  // We keep track of this since the response for a key can be truncated at the end of
  // 'buffer_', and we need to continue processing the same key after obtaining relevant
  // information from the next batch of bytes from Memcached.
  std::string cur_key_;

  // This is the last state seen in a buffer being processed. It's used to find out where
  // our buffer got truncated, so we can continue from there.
  DataBufferSlice::ResponseFormatState broken_buffer_state_;

  // If our buffer filled up before we could receive all the data for a command,
  // we need to make sure to drain the socket before sending the next command.
  bool need_drain_socket_;

  // Responsible for managing all the files that we will write data to.
  std::unique_ptr<RotatingFile> rotating_data_files_;
};

} // namespace memcachedumper
