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
