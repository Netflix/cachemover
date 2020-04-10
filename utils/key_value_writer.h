#pragma once

#include "utils/file_util.h"
#include "utils/memcache_utils.h"

#include <string>
#include <unordered_map>

namespace memcachedumper {

// Forward declarations.
class Socket;

typedef std::unordered_map<std::string, std::unique_ptr<McData>> McDataMap;

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
  void ProcessKey(McData* mc_key);

  void PrintKeys();

  uint64_t num_processed_keys() { return num_processed_keys_; }

 private:

  // Get values from Memcached for keys present in mcdata_entries_ map.
  // If 'flush' is true, it will get all the keys from mcdata_entries_ even if
  // our buffer is not used to 'capacity_'.
  void BulkGetKeys(bool flush);

  // Process the response from Memcached and populate mcdata_entries_ appropriately.
  // Returns 'true' if the entire response was processed, i.e. if "END\r\n" was seen
  // in 'buffer'.
  bool ProcessBulkResponse(uint8_t* buffer, int32_t bufsize);

  // Writes entires marked as complete from 'mcdata_entries_' to the final output
  // file.
  void WriteCompletedEntries(uint32_t num_complete_entries);

  // The prefix to use for every file created by this object.
  std::string data_file_prefix_;
  // Name of the thread that's operating on this object.
  const std::string owning_thread_name_;
  // The address of the buffer to use while getting values from Memcached.
  uint8_t* buffer_begin_;
  // A pointer to the first free byte in buffer_begin_.
  uint8_t* buffer_current_;
  // The size of 'buffer_'.
  size_t capacity_;
  // Maximum size of file to create.
  uint64_t max_file_size_;
  // Socket to talk to Memcached.
  Socket* mc_sock_;
  // Map of key name to McData entries.
  //std::unordered_map<std::string, std::unique_ptr<McData>> mcdata_entries_;
  McDataMap mcdata_entries_;

  // While parsing responses from Memcached, this keeps track of the current key
  // being processed.
  // We keep track of this since the response for a key can be truncated at the end of
  // 'buffer_', and we need to continue processing the same key after obtaining relevant
  // information from the next batch of bytes from Memcached.
  std::string cur_key_;

  // This is the last state seen in a buffer being processed. It's used to find out where
  // our buffer got truncated, so we can continue from there.
  DataBufferSlice::ResponseFormatState broken_buffer_state_;

  // Was the last buffer processed a partial buffer?
  bool last_buffer_partial_;

  // Responsible for managing all the files that we will write data to.
  std::unique_ptr<RotatingFile> rotating_data_files_;

  uint64_t num_processed_keys_ = 0;
};

} // namespace memcachedumper
