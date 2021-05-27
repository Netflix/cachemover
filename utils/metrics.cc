#include "utils/metrics.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/writer.h>

namespace memcachedumper {

MonotonicStopWatch DumpMetrics::total_elapsed_;
uint64_t DumpMetrics::total_metadump_keys_ = 0;
std::atomic_uint64_t DumpMetrics::total_keys_processed_ = 0;
std::atomic_uint64_t DumpMetrics::total_keys_ignored_ = 0;
std::atomic_uint64_t DumpMetrics::total_keys_missing_ = 0;
std::atomic_uint64_t DumpMetrics::total_keys_filtered_ = 0;

std::string DumpMetrics::MetricsAsJsonString() {
  rapidjson::Document root;
  // Use as object instead of array
  root.SetObject();
  // Allocator for object
  rapidjson::Document::AllocatorType& allocator = root.GetAllocator();

  rapidjson::Value kv_metrics_obj(rapidjson::kObjectType);
  kv_metrics_obj.AddMember("total",
      DumpMetrics::total_metadump_keys(), allocator);
  kv_metrics_obj.AddMember("dumped",
      DumpMetrics::total_keys_processed(), allocator);
  kv_metrics_obj.AddMember("not_found",
      DumpMetrics::total_keys_missing(), allocator);
  kv_metrics_obj.AddMember("skipped",
      DumpMetrics::total_keys_ignored(), allocator);
  kv_metrics_obj.AddMember("filtered",
      DumpMetrics::total_keys_filtered(), allocator);
  root.AddMember("keyvalue_metrics", kv_metrics_obj, allocator);

  std::string elapsed_str = time_elapsed_str();
  rapidjson::Value elapsed_val;
  elapsed_val.SetString(elapsed_str.c_str(), elapsed_str.length(), allocator);
  root.AddMember("time_elapsed", elapsed_val, allocator);

  rapidjson::StringBuffer strbuf;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
  root.Accept(writer);

  return strbuf.GetString();

}

} // namespace memcachedumper
