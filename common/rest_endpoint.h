/**
 *
 *  Copyright 2018 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

#include "tasks/task_scheduler.h"

#include <pistache/http.h>
#include <pistache/description.h>
#include <pistache/endpoint.h>

#include <memory>
#include <string>
#include <thread>

using namespace Pistache;
using namespace Rest;

namespace memcachedumper {

class RESTServer {
 public:
  RESTServer(TaskScheduler* task_scheduler);
  void Init();
  void HandleGet(const Rest::Request&, Http::ResponseWriter response);
  void Shutdown();

 private:
  Pistache::Port port_;
  Pistache::Address addr_;
  Pistache::Http::Endpoint rest_server_;

  //Pistache::Rest::Description desc_;
  Pistache::Rest::Router router_;

  // Pointer to the task scheduler (for metrics)
  // TODO: Abstract away metrics to separate class
  TaskScheduler* task_scheduler_;

  std::thread server_thread_;
};

} // namespace memcachedumper
