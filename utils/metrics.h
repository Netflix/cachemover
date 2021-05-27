#pragma once

#include "utils/stopwatch.h"

#include <atomic>
#include <string>

#include <stdint.h>

namespace memcachedumper {

// TODO: Move remaining metrics out of TaskScheduler into DumpMetrics.
class DumpMetrics {
 public:
  static MonotonicStopWatch& total_msw() { return total_elapsed_; }
  static std::string time_elapsed_str() { return total_elapsed_.HumanElapsedStr(); }
  static uint64_t total_metadump_keys() { return total_metadump_keys_; }
  static uint64_t total_keys_processed() { return total_keys_processed_; }
  static uint64_t total_keys_ignored() { return total_keys_ignored_; }
  static uint64_t total_keys_missing() { return total_keys_missing_; }
  static uint64_t total_keys_filtered() { return total_keys_filtered_; }

  static void increment_total_metadump_keys(uint64_t num_keys) {
    total_metadump_keys_ += num_keys;
  }
  static void update_total_keys_processed(uint64_t num_keys) {
    total_keys_processed_ = num_keys;
  }
  static void update_total_keys_ignored(uint64_t num_keys) {
    total_keys_ignored_ = num_keys;
  }
  static void update_total_keys_missing(uint64_t num_keys) {
    total_keys_missing_ = num_keys;
  }
  static void update_total_keys_filtered(uint64_t num_keys) {
    total_keys_filtered_ = num_keys;
  }

  // Persist metrics to a file.
  static void PersistMetrics();

  // Pretty format the metrics as a JSON string and return it.
  static std::string MetricsAsJsonString();

 private:

  // Tracks the total time spent on the dumping process so far.
  static MonotonicStopWatch total_elapsed_;

  // Metric to track the number of total keys returned from the metadump stage.
  // Note: This is not an 'atomic' since we don't expect more than one thread to
  // update this.
  static uint64_t total_metadump_keys_;

  // The remaining metrics are 'atomic' as they can be updated concurrently.
  // Metric to track the number of total keys processed so far.
  static std::atomic_uint64_t total_keys_processed_;
  // Metric to track the number of total keys ignored for any reason so far.
  static std::atomic_uint64_t total_keys_ignored_;
  // Metric to track the number of total keys missing so far.
  static std::atomic_uint64_t total_keys_missing_;
  // Metric to track the number of total keys filtered out so far.
  static std::atomic_uint64_t total_keys_filtered_;

};

} // namespace memcachedumper
