#include "dumper/dumper.h"

#include "common/logger.h"
#include "common/rest_endpoint.h"
#include "tasks/metadump_task.h"
#include "tasks/resume_task.h"
#include "tasks/task.h"
#include "tasks/task_scheduler.h"
#include "utils/file_util.h"
#include "utils/mem_mgr.h"
#include "utils/memcache_utils.h"
#include "utils/net_util.h"
#include "utils/socket_pool.h"

#include <iostream>
#include <sstream>
#include <string>

#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/CreateQueueResult.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/GetQueueUrlResult.h>

using std::string;
using std::string_view;

namespace memcachedumper {

void DumperOptions::set_memcached_hostname(string_view memcached_hostname) {
  memcached_hostname_ = memcached_hostname;
}

void DumperOptions::set_memcached_port(int memcached_port) {
  memcached_port_ = memcached_port;
}

void DumperOptions::set_num_threads(int num_threads) {
  num_threads_ = num_threads;
}

void DumperOptions::set_chunk_size(uint64_t chunk_size) {
  chunk_size_ = chunk_size;
}

void DumperOptions::set_max_memory_limit(uint64_t max_memory_limit) {
  max_memory_limit_ = max_memory_limit;
}

void DumperOptions::set_max_key_file_size(uint64_t max_key_file_size) {
  max_key_file_size_ = max_key_file_size;
}

void DumperOptions::set_max_data_file_size(uint64_t max_data_file_size) {
  max_data_file_size_ = max_data_file_size;
}

void DumperOptions::set_log_file_path(string_view log_file_path) {
  log_file_path_ = log_file_path;
}

void DumperOptions::set_output_dir_path(string_view output_dir_path) {
  output_dir_path_ = output_dir_path;
}

void DumperOptions::set_bulk_get_threshold(uint32_t bulk_get_threshold) {
  bulk_get_threshold_ = bulk_get_threshold;
}

void DumperOptions::set_only_expire_after(int only_expire_after) {
  only_expire_after_ = only_expire_after;
}

void DumperOptions::set_resume_mode(bool resume_mode) {
  resume_mode_ = resume_mode;
}

void DumperOptions::set_is_s3_dump(bool is_s3_dump) {
  is_s3_dump_ = is_s3_dump;
}

void DumperOptions::set_s3_bucket_name(std::string s3_bucket) {
  s3_bucket_ = s3_bucket;
}

void DumperOptions::set_s3_final_path(std::string s3_path) {
  s3_path_ = s3_path;
}

void DumperOptions::set_req_id(std::string req_id) {
  req_id_ = req_id;
}

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

  // TODO: Create AwsUtils class and make this function use that.
  Aws::String queue_name = MemcachedUtils::GetSQSQueueName();

  Aws::SQS::Model::GetQueueUrlRequest gqu_req;
  gqu_req.SetQueueName(queue_name);

  auto gqu_out = sqs_client_.GetQueueUrl(gqu_req);
  if (gqu_out.IsSuccess()) {
    LOG("SQS Queue {0} already exists with URL: {1}", queue_name,
        gqu_out.GetResult().GetQueueUrl());

    // If the queue already exists, we can simply start publishing to it when we choose.
    MemcachedUtils::SetSQSQueueURL(gqu_out.GetResult().GetQueueUrl());
    return Status::OK();
  } else {
    // An error likely means that the queue does not exist. So we must create it.
    LOG("Error getting url for queue {0} : {1}", queue_name,
        gqu_out.GetError().GetMessage());
  }

  LOG("Setting up a new queue...");
  Aws::SQS::Model::CreateQueueRequest cq_req;
  cq_req.SetQueueName(queue_name);
  auto cq_out = sqs_client_.CreateQueue(cq_req);
  if (cq_out.IsSuccess()) {
    LOG("Successfully created SQS queue: {0}", queue_name);

    // If the queue is successfully created, we can simply start publishing to it when we choose.
    MemcachedUtils::SetSQSQueueURL(cq_out.GetResult().GetQueueUrl());
    return Status::OK();
  } else {
    LOG_ERROR("Error creating queue {0} : {1}", queue_name,
        cq_out.GetError().GetMessage());
  }

  // If we failed to create a queue, it's likely that we raced with another dumper instance, so
  // we confirm if it is created or not.
  gqu_out = sqs_client_.GetQueueUrl(gqu_req);
  if (gqu_out.IsSuccess()) {
    LOG("SQS Queue {0} already exists with URL: {1}", queue_name,
        gqu_out.GetResult().GetQueueUrl());

    // If the queue already exists, we can simply start publishing to it when we choose.
    MemcachedUtils::SetSQSQueueURL(gqu_out.GetResult().GetQueueUrl());
    return Status::OK();
  } else {
    // If we're unable to create the queue and not see it, something must be wrong.
    LOG("Error getting url for queue {0} : {1}", queue_name,
        gqu_out.GetError().GetMessage());
    return Status::NetworkError("Unable to set-up SQS queue ",
        gqu_out.GetError().GetMessage());
  }
}

Status Dumper::Init() {

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

  if (opts_.is_s3_dump()) {
    LOG("Dump target set to S3. S3 Bucket: {0} ; S3 Path: {1}", opts_.s3_bucket(), opts_.s3_path());
    MemcachedUtils::SetS3Bucket(opts_.s3_bucket());
    MemcachedUtils::SetS3Path(opts_.s3_path());
    RETURN_ON_ERROR(InitSQS());

    MemcachedUtils::SetS3Client(&s3_client_);
    MemcachedUtils::SetSQSClient(&sqs_client_);
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
    task_scheduler_->total_keys_processed(),
    task_scheduler_->total_keys_ignored(),
    task_scheduler_->total_keys_missing(),
    total_msw_.HumanElapsedStr());
  dtask.Execute();

  rest_server_->Shutdown();
  std::stringstream final_metrics;
  final_metrics << std::endl
      << "All tasks completed..." << std::endl
      << "-------------------------" << std::endl
      << " -Total keys dumped: " << task_scheduler_->total_keys_processed() << std::endl
      << " -Total keys ignored: " << task_scheduler_->total_keys_ignored() << std::endl
      << " -Total keys missing: " << task_scheduler_->total_keys_missing() << std::endl
      << " -Time taken: " << total_msw_.HumanElapsedStr() << std::endl;
  LOG(final_metrics.str());
  LOG("Status: All tasks completed. Exiting...");
}

} // namespace memcachedumper
