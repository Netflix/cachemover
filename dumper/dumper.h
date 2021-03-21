#pragma once

#include "dumper/dumper_config.h"
#include "utils/status.h"
#include "utils/stopwatch.h"

#include <memory>
#include <string>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/sqs/SQSClient.h>

namespace memcachedumper {

class MemoryManager;
class RESTServer;
class Socket;
class SocketPool;
class TaskScheduler;

class Dumper {
 public:
  Dumper(DumperOptions& opts);

  ~Dumper();

  Aws::S3::S3Client* GetS3Client() { return &s3_client_; }
  Aws::SQS::SQSClient* GetSQSClient() { return &sqs_client_; }

  MemoryManager *mem_mgr() { return mem_mgr_.get(); }

  std::string TimeElapsed() { return total_msw_.HumanElapsedStr(); }

  // Initializes the dumper by connecting to memcached.
  Status Init();

  // Initialize the SQS queue.
  Status InitSQS();

  // Starts the dumping process.
  void Run();

  // Returns a socket to memcached from the socket pool.
  Socket* GetMemcachedSocket();

  // Releases a memcached socket back to the socket pool.
  void ReleaseMemcachedSocket(Socket *sock);

 private:

  // Set up the output directories and make sure they're empty.
  Status CreateAndValidateOutputDirs();

  std::string memcached_hostname_;

  DumperOptions opts_;

  // Pool of sockets to talk to memcached.
  std::unique_ptr<SocketPool> socket_pool_;

  // Owned memory manager.
  std::unique_ptr<MemoryManager> mem_mgr_;

  // The task scheduler that will carry out all the work.
  std::unique_ptr<TaskScheduler> task_scheduler_;

  // A REST Server to report metrics.
  std::unique_ptr<RESTServer> rest_server_;

  Aws::Client::ClientConfiguration s3_config_;
  Aws::S3::S3Client s3_client_;
  Aws::SQS::SQSClient sqs_client_;

  // Stopwatch to run for the entire duration of the dumper.
  MonotonicStopWatch total_msw_;

};

} // namespace memcachedumper
