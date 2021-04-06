#include "common/logger.h"
#include "common/rest_endpoint.h"
#include "dumper/dumper.h"
#include "utils/metrics.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <iostream>

using namespace Pistache;
using namespace Rest;
using namespace rapidjson;

namespace memcachedumper {

RESTServer::RESTServer(TaskScheduler* task_scheduler)
  : port_(7777),
    addr_(Pistache::Ipv4::any(), port_),
    rest_server_(addr_),
    task_scheduler_(task_scheduler),
    server_thread_(&RESTServer::Init, this) {
}

void RESTServer::Init() {
  // Init with 1 thread.
  rest_server_.init(Http::Endpoint::options().threads(1));

  Routes::Get(router_, "/metrics", Routes::bind(&RESTServer::HandleGet, this));
  rest_server_.setHandler(router_.handler());

  rest_server_.serve();

}

void RESTServer::HandleGet(const Rest::Request& request,
    Http::ResponseWriter response) {

  rapidjson::Document root;
  // Use as object instead of array
  root.SetObject();
  // Allocator for object
  rapidjson::Document::AllocatorType& allocator = root.GetAllocator();

  rapidjson::Value kv_metrics_obj(rapidjson::kObjectType);
  kv_metrics_obj.AddMember("total",
      DumpMetrics::total_keys(), allocator);
  kv_metrics_obj.AddMember("dumped",
      task_scheduler_->total_keys_processed(), allocator);
  kv_metrics_obj.AddMember("not_found",
      task_scheduler_->total_keys_missing(), allocator);
  kv_metrics_obj.AddMember("skipped",
      task_scheduler_->total_keys_ignored(), allocator);
  kv_metrics_obj.AddMember("filtered",
      task_scheduler_->total_keys_filtered(), allocator);
  root.AddMember("keyvalue_metrics", kv_metrics_obj, allocator);

  std::string elapsed_str = task_scheduler_->dumper()->TimeElapsed();
  Value elapsed_val;
  elapsed_val.SetString(elapsed_str.c_str(), elapsed_str.length(), allocator);
  root.AddMember("time_elapsed", elapsed_val, allocator);

  rapidjson::StringBuffer strbuf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
  root.Accept(writer);

  LOG("Metrics requested: {0}", strbuf.GetString());
  response.send(Http::Code::Ok, strbuf.GetString());
}

void RESTServer::Shutdown() {
  rest_server_.shutdown();
  server_thread_.join();
}

} // namespace memcachedumper
