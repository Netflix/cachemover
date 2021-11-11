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
