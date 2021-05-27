#include "common/logger.h"
#include "common/rest_endpoint.h"
#include "dumper/dumper.h"
#include "utils/metrics.h"

#include <iostream>

using namespace Pistache;
using namespace Rest;

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
  std::string metrics_str = DumpMetrics::MetricsAsJsonString();
  LOG("Metrics requested: {0}", metrics_str);
  response.send(Http::Code::Ok, metrics_str);
}

void RESTServer::Shutdown() {
  rest_server_.shutdown();
  server_thread_.join();
}

} // namespace memcachedumper
