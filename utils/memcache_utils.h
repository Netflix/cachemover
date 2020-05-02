#pragma once

#include "utils/slice.h"

#include <memory>
#include <string>
#include <unordered_map>

// Items that are expiring in these many seconds will not be dumped.
#define EXPIRE_THRESHOLD_DELTA_S 9000

// Ignore a key if we tried to get it these many times unsuccessfully.
#define MAX_GET_ATTEMPTS 3

namespace memcachedumper {

class McData;

typedef std::unordered_map<std::string, std::unique_ptr<McData>> McDataMap;

class MemcachedUtils {
 public:
  static std::string CraftBulkGetCommand(McDataMap* pending_keys, const int max_keys);
  static bool KeyExpiresSoon(time_t now, long int key_expiry) {
    // TODO: Is this protable?
    return (key_expiry <= now + EXPIRE_THRESHOLD_DELTA_S);
  }

  static void lol();
};


class McData {
 public:
  McData(char *key, size_t keylen, int32_t expiry);
  void setValue(const char* data_ptr, size_t size);
  void setValueLength(size_t value_len) { value_len_ = value_len; }
  void setFlags(int32_t flags) { flags_ = flags; }

  void printValue();

  std::string& key() { return key_; }
  int32_t expiry() { return expiry_; }

  char* Value() { return data_->mutable_data(); }
  size_t ValueLength() { return value_len_; }

  void MarkComplete() { complete_ = true; }

  void set_get_complete(bool get_complete) {
    ++get_attempts_;
    get_complete_ = get_complete;
  }
  inline bool get_complete() { return get_complete_; }
  // Returns 'false' if this McData is marked as incomplete, i.e. one or
  // more required fields are not present/completely entered.
  bool Complete() { return complete_; }

  // If we tried to get a key for MAX_GET_ATTEMPTS unsuccussfully, we
  // consider the key evicted or expired.
  bool PossiblyEvicted() { return get_attempts_ >= MAX_GET_ATTEMPTS; }

 private:
  std::string key_;
  int32_t expiry_;
  int32_t flags_;
  size_t value_len_;
  std::unique_ptr<Slice> data_;

  bool get_complete_;
  bool complete_;
  int get_attempts_;
};

class MetaBufferSlice : public Slice {
 public:
  MetaBufferSlice(const char* d, size_t n)
    : Slice(d, n),
      pending_data_(d),
      start_copy_pos_(0),
      slice_end_(d + n) {

  }

  char* buf_begin_fill() {
    return const_cast<char*>(data() + start_copy_pos_);
  }

  size_t free_bytes() {
    return size() - start_copy_pos_;
  }

  const char* next_key_pos() {
    const char* pos = strstr(pending_data_, "key=");
    if (pos && pos < slice_end_) {
      MarkProcessedUntil(pos + 4); // Skip 'key='
      return pos;
    }
    return nullptr;
  }
  const char* next_exp_pos() {
    const char* pos = strstr(pending_data_, "exp=");
    if (pos && pos < slice_end_) {
      MarkProcessedUntil(pos + 4); // Skip 'exp='
      return pos;
    }
    return nullptr;
  }
  const char* next_la_pos() {
    const char* pos = strstr(pending_data_, "la=");
    if (pos && pos < slice_end_) {
      MarkProcessedUntil(pos + 3); // Skip 'la='
      return pos;
    }
    return nullptr;
  }
  const char* next_newline() {
    const char* pos = strstr(pending_data_, "\n");
    if (pos && pos < slice_end_) {
      MarkProcessedUntil(pos + 1); // Skip '\n'
      return pos;
    }
    return nullptr;
  }

  void CopyRemainingToStart(const char* copy_from) {
    //ASSERT(copy_from > data() && copy_from < data() + size());

    size_t num_bytes_to_copy = data() + size() - copy_from;
    //ASSERT(num_bytes_to_copy < (copy_from - data());

    memcpy(const_cast<char*>(data()), copy_from, num_bytes_to_copy);
    start_copy_pos_ = num_bytes_to_copy;

    // Zero out the remaining bytes so that we don't accidentally parse them again.
    bzero(const_cast<char*>(data() + start_copy_pos_), free_bytes());
  }

  const char* pending_data() { return pending_data_; }

 private:
  inline void MarkProcessedUntil(const char *buf_ptr) {
    pending_data_ = buf_ptr;
  }
 
  const char* pending_data_;
  uint32_t start_copy_pos_;

  // Points to the end of the slice.
  const char* const slice_end_;
};

class DataBufferSlice : public Slice {
 public:
  DataBufferSlice(const char* d, size_t n)
    : Slice(d, n),
      parse_state_(ResponseFormatState::VALUE_DELIM),
      pending_data_(d),
      start_copy_pos_(0) {

  }

  enum class ResponseFormatState {
    VALUE_DELIM = 0,
    KEY_NAME = 1,
    FLAGS = 2,
    NUM_DATA_BYTES = 3,
    DATA = 4,
    TOTAL_NUM_STATES = 5
  };

  char* buf_begin_fill() {
    return const_cast<char*>(data() + start_copy_pos_);
  }

  size_t free_bytes() {
    return size() - start_copy_pos_;
  }

  const char* next_value_delim() {
    //const char* pos = strstr(pending_data_, "VALUE ");
    int32_t n_pending = bytes_pending();
    const char* pos = static_cast<const char*>(memmem(pending_data_, n_pending, "VALUE ", 6));
    if (pos) MarkProcessedUntil(pos + 6); // Skip 'VALUE '
    return pos;
  }
  const char* next_whitespace() {
    const char* pos = strstr(pending_data_, " ");
    if (pos) MarkProcessedUntil(pos + 1); // Skip ' '
    return pos;
  }
  const char* next_crlf() {
    const char* pos = strstr(pending_data_, "\r\n");
    if (pos) MarkProcessedUntil(pos + 2); // Skip '\r\n'
    return pos;
  }
  const char* process_value(size_t value_size) {
    if (pending_data_ + value_size > data() + size()) {
      return nullptr;
    }
    MarkProcessedUntil(pending_data_ + value_size + 2);
    return pending_data_ + value_size;
  }

  const char* pending_data() { return pending_data_; }

  int32_t bytes_pending() { return &data()[size()] - pending_data_; }

  bool reached_end() {
    return !strncmp(&data()[size() - 5], "END\r\n", 5);
  }
  bool reached_error() {
    return !strncmp(&data()[size() - 7], "ERROR\r\n", 7);
  }

  ResponseFormatState parse_state() { return parse_state_; }

 private:

  inline void StepStateMachine() {
    int16_t cur_state = static_cast<int16_t>(parse_state_);
    int16_t total_states = static_cast<int16_t>(ResponseFormatState::TOTAL_NUM_STATES);

    parse_state_ = static_cast<ResponseFormatState>((cur_state + 1) % total_states);
  }

  inline void MarkProcessedUntil(const char *buf_ptr) {
    pending_data_ = buf_ptr;
    StepStateMachine();
  }
 
  ResponseFormatState parse_state_;
  const char* pending_data_;
  uint32_t start_copy_pos_;
};

} // namespace memcachedumper
