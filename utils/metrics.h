#pragma once

#include <stdint.h>

namespace memcachedumper {

// TODO: Move remaining metrics out of TaskScheduler into DumpMetrics.
class DumpMetrics {
 public:
  static uint64_t total_keys() { return total_keys_; }

  static void update_total_keys(uint64_t num_keys) { total_keys_ += num_keys; }
 private:
  // Metric to track the number of total keys returned from the metadump stage.
  static uint64_t total_keys_;
};

} // namespace memcachedumper
