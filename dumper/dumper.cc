#include "dumper/dumper_config.h"
#include "dumper/dumper.h"

#include "common/logger.h"
#include "common/rest_endpoint.h"
#include "tasks/metadump_task.h"
#include "tasks/resume_task.h"
#include "tasks/task.h"
#include "tasks/task_scheduler.h"
#include "utils/aws_utils.h"
#include "utils/file_util.h"
#include "utils/mem_mgr.h"
#include "utils/metrics.h"
#include "utils/memcache_utils.h"
#include "utils/net_util.h"
#include "utils/socket_pool.h"

#include <sstream>

#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/CreateQueueResult.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/GetQueueUrlResult.h>

namespace memcachedumper {

Dumper::Dumper(DumperOptions& opts)
  : opts_(opts),
    s3_client_(s3_config_) {
  std::stringstream options_log;
  options_log << "Starting dumper with options: " << std::endl
            << "Hostname: " << opts_.memcached_hostname() << std::endl
            << "Port: " << opts_.memcached_port() << std::endl
            << "Num threads: " << opts_.num_threads() << std::endl
            << "Chunk size: " << opts_.chunk_size() << std::endl
            << "Max memory limit: " << opts_.max_memory_limit() << std::endl
            << "Max key file size: " << opts_.max_key_file_size() << std::endl
            << "Max data file size: " << opts_.max_data_file_size() << std::endl
            << "Bulk get threshold: " << opts_.bulk_get_threshold() << std::endl
            << "Output directory: " << opts_.output_dir_path() << std::endl
            << std::endl;
  LOG(options_log.str());

  socket_pool_.reset(new SocketPool(
      opts_.memcached_hostname(), opts_.memcached_port(), opts_.num_threads() + 1));
  mem_mgr_.reset(new MemoryManager(
      opts_.chunk_size(), opts_.max_memory_limit() / opts_.chunk_size()));
}

Dumper::~Dumper() = default;

Status Dumper::CreateAndValidateOutputDirs() {
  RETURN_ON_ERROR(FileUtils::CreateDirectory(MemcachedUtils::OutputDirPath()));
  RETURN_ON_ERROR(FileUtils::CreateDirectory(MemcachedUtils::GetKeyFilePath()));
  RETURN_ON_ERROR(FileUtils::CreateDirectory(MemcachedUtils::GetDataStagingPath()));
  RETURN_ON_ERROR(FileUtils::CreateDirectory(MemcachedUtils::GetDataFinalPath()));

  return Status::OK();
}

Status Dumper::InitSQS() {
  Aws::String queue_name = AwsUtils::GetSQSQueueName();

  std::string queue_url;
  Status get_url_status = AwsUtils::GetSQSUrlFromName(queue_name, &queue_url);
  if (get_url_status.ok()) {
    LOG("SQS Queue {0} already exists with URL: {1}", queue_name, queue_url);

    // If the queue already exists, we can simply start publishing to it when we choose.
    AwsUtils::SetCachedSQSQueueURL(queue_url);
    return Status::OK();
  }

  LOG("Queue with name {0} not found. Setting up a new queue...", queue_name);
  Status create_queue_status = AwsUtils::CreateNewSQSQueue(queue_name, &queue_url);
  if (create_queue_status.ok()) {
    LOG("Successfully created SQS Queue {0} with URL: {1}", queue_name, queue_url);

    // If the queue is successfully created, we can simply start publishing to it when we choose.
    AwsUtils::SetCachedSQSQueueURL(queue_url);
    return Status::OK();
  } else {
    LOG_ERROR("Could not create SQS Queue {0}. Reason: {1}", queue_name, create_queue_status.ToString());
  }

  // If we were unable to create the queue, it's likely that we raced with another dumper instance.
  // Confirm that the queue exists.
  get_url_status = AwsUtils::GetSQSUrlFromName(queue_name, &queue_url);
  if (get_url_status.ok()) {
    LOG("SQS Queue {0} already exists with URL: {1}", queue_name, queue_url);

    // If the queue already exists, we can simply start publishing to it when we choose.
    AwsUtils::SetCachedSQSQueueURL(queue_url);
  } else {
    LOG_ERROR("Could not create or find SQS Queue {0}. Reason: {1}", queue_name,
        get_url_status.ToString());
    return get_url_status;
  }

  LOG("SQS successfully initialized.");
  return Status::OK();
}

Status Dumper::Init() {

  // update the ulimits for dumping the core for this process
  // copied from memcached source
  struct rlimit rlim;
  struct rlimit rlim_new;

  // First try raising to infinity; if that fails, try bringing
  // the soft limit to the hard.
  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
      rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
      if (setrlimit(RLIMIT_CORE, &rlim_new) != 0) {
          // failed. try raising just to the old max
          rlim_new.rlim_cur = rlim_new.rlim_max =
              rlim.rlim_max;
          (void) setrlimit(RLIMIT_CORE, &rlim_new);
      }
  }

  // getrlimit again to see what we ended up with. Only fail if
  // the soft limit ends up 0, because then no core files will be
  // created at all.
  if ((getrlimit(RLIMIT_CORE, &rlim) != 0) || rlim.rlim_cur == 0) {
      LOG_ERROR("failed to ensure corefile creation");
      return Status::CoreDumpError("failed to ensure corefile creation");
  }


  // If we've been given a list of destination IPs to filter the dump for,
  // initialize the key filter.
  if (opts_.dest_ips().size() > 0) {
    LOG("--dest_ips and --all_ips provided. Initializing key filter.");
    MemcachedUtils::SetDestIps(opts_.dest_ips());
    MemcachedUtils::SetAllIps(opts_.all_ips());
    RETURN_ON_ERROR(MemcachedUtils::InitKeyFilter(opts_.ketama_bucket_size()));
  }

  MemcachedUtils::SetReqId(opts_.req_id());
  MemcachedUtils::SetOutputDirPath(opts_.output_dir_path());
  MemcachedUtils::SetBulkGetThreshold(opts_.bulk_get_threshold());
  if (opts_.only_expire_after() > 0) {
    MemcachedUtils::SetOnlyExpireAfter(opts_.only_expire_after());
  }
  MemcachedUtils::SetMaxDataFileSize(opts_.max_data_file_size());
  LOG("Keyfile path: {0}", MemcachedUtils::GetKeyFilePath());
  LOG("Data staging path: {0}", MemcachedUtils::GetDataStagingPath());
  LOG("Data final path: {0}", MemcachedUtils::GetDataFinalPath());

  if (!opts_.is_resume_mode()) {
    RETURN_ON_ERROR(CreateAndValidateOutputDirs());
  }
  RETURN_ON_ERROR(socket_pool_->PrimeConnections());
  RETURN_ON_ERROR(mem_mgr_->PreallocateChunks());

  // TODO: Validate if we have enough free space to run the dump smoothly.
  uint64_t free_space = FileUtils::GetSpaceAvailable(opts_.output_dir_path());
  LOG("Amount of free space on disk: {0} MB", free_space / 1024 / 1024);

  if (opts_.is_s3_dump()) {
    LOG("Dump target set to S3. S3 Bucket: {0} ; S3 Path: {1}", opts_.s3_bucket(), opts_.s3_path());
    AwsUtils::SetS3Client(&s3_client_);
    AwsUtils::SetSQSClient(&sqs_client_);
    AwsUtils::SetS3Bucket(opts_.s3_bucket());
    AwsUtils::SetS3Path(opts_.s3_path());
    AwsUtils::SetSQSQueueName(opts_.sqs_queue_name());
    RETURN_ON_ERROR(InitSQS());
  }

  task_scheduler_.reset(new TaskScheduler(opts_.num_threads(), this));
  task_scheduler_->Init();

  rest_server_.reset(new RESTServer(task_scheduler_.get()));
  return Status::OK();
}

Socket* Dumper::GetMemcachedSocket() {
  return socket_pool_->GetSocket();
}

void Dumper::ReleaseMemcachedSocket(Socket *sock) {
  return socket_pool_->ReleaseSocket(sock);
}

void Dumper::Run() {

  {
    SCOPED_STOP_WATCH(&total_msw_);

    if (opts_.is_resume_mode()) {
      // TODO
      ResumeTask *rtask = new ResumeTask(opts_.is_s3_dump());
      task_scheduler_->SubmitTask(rtask);
    } else {
      MetadumpTask *mtask = new MetadumpTask(
          0, MemcachedUtils::GetKeyFilePath(), opts_.max_key_file_size(), mem_mgr_.get(),
          opts_.is_s3_dump());
      task_scheduler_->SubmitTask(mtask);
    }

    task_scheduler_->WaitUntilTasksComplete();
  }

  // Output the "DONE" file.
  // TODO: (nit) Ideally would be submitted to the task scheduler.
  DoneTask dtask(
    DumpMetrics::total_keys(),
    task_scheduler_->total_keys_processed(),
    task_scheduler_->total_keys_ignored(),
    task_scheduler_->total_keys_missing(),
    task_scheduler_->total_keys_filtered(),
    total_msw_.HumanElapsedStr());
  dtask.Execute();

  rest_server_->Shutdown();
  std::stringstream final_metrics;
  final_metrics << std::endl
      << "All tasks completed..." << std::endl
      << "-------------------------" << std::endl
      << " -Total keys from metadump: " << DumpMetrics::total_keys() << std::endl
      << "-------------------------" << std::endl
      << " -Total keys dumped: " << task_scheduler_->total_keys_processed() << std::endl
      << " -Total keys ignored: " << task_scheduler_->total_keys_ignored() << std::endl
      << " -Total keys missing: " << task_scheduler_->total_keys_missing() << std::endl
      << " -Total keys filtered: " << task_scheduler_->total_keys_filtered() << std::endl
      << " -Time taken: " << total_msw_.HumanElapsedStr() << std::endl;
  LOG(final_metrics.str());
  LOG("Status: All tasks completed. Exiting...");
}

} // namespace memcachedumper
