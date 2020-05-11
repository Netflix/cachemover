#include "common/rest_endpoint.h"

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
    task_scheduler_(task_scheduler) {
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

  rapidjson::Value metrics_obj(rapidjson::kObjectType);
  metrics_obj.AddMember("dumped",
      task_scheduler_->total_keys_processed(), allocator);
  metrics_obj.AddMember("missed",
      task_scheduler_->total_keys_missing(), allocator);
  metrics_obj.AddMember("ignored",
      task_scheduler_->total_keys_ignored(), allocator);

  root.AddMember("metrics", metrics_obj, allocator);

  rapidjson::StringBuffer strbuf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
  root.Accept(writer);

  std::cout << "Metrics requested: " << strbuf.GetString() << std::endl;
  response.send(Http::Code::Ok, strbuf.GetString());
}

} // namespace memcachedumper
