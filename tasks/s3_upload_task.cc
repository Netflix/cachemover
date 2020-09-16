#include "tasks/s3_upload_task.h"

#include "common/logger.h"
#include "tasks/task_scheduler.h"
#include "tasks/task_thread.h"
#include "utils/aws_utils.h"
#include "utils/mem_mgr.h"
#include "utils/memcache_utils.h"
#include "utils/socket.h"

#include <unistd.h>
#include <string.h>

#include <experimental/filesystem>
#include <fstream>
#include <sstream>

#include <aws/core/Aws.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/ReceiveMessageResult.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sqs/model/SendMessageResult.h>

namespace fs = std::experimental::filesystem;
namespace memcachedumper {

S3UploadFileTask::~S3UploadFileTask() = default;

S3UploadFileTask::S3UploadFileTask(std::string fq_local_path, std::string filename)
  : fq_local_path_(fq_local_path),
    filename_(filename),
    upload_status_(Status::OK()) {
}

Status S3UploadFileTask::SendSQSNotification(std::string s3_file_uri) {
  Aws::SQS::Model::SendMessageRequest sm_req;
  std::string queue_url = AwsUtils::GetCachedSQSQueueURL();
  sm_req.SetQueueUrl(queue_url);

  std::string msg_body;
  RETURN_ON_ERROR(AwsUtils::SQSBodyForS3(s3_file_uri, &msg_body));
  sm_req.SetMessageBody(msg_body);

  auto sm_out = AwsUtils::GetSQSClient()->SendMessage(sm_req);
  if (sm_out.IsSuccess()) {
    LOG("Successfully sent SQS message to {0} for file {1}", queue_url, filename_);
  } else {
    return Status::NetworkError("Error sending SQS message to : " + queue_url,
        sm_out.GetError().GetMessage());
  }

  return Status::OK();
}

void S3UploadFileTask::Execute() {
  std::string s3_key_name = AwsUtils::GetS3Path() + "/" +
      MemcachedUtils::GetReqId() + "/" + filename_;

  const std::shared_ptr<Aws::IOStream> input_data =
  Aws::MakeShared<Aws::FStream>("SampleAllocationTag",
    fq_local_path_.c_str(),
    std::ios_base::in | std::ios_base::binary);

  Aws::S3::Model::PutObjectRequest object_request;

  object_request.SetBucket(AwsUtils::GetS3Bucket());
  object_request.SetKey(s3_key_name);
  object_request.SetBody(input_data);

  // Put the object
  auto put_object_outcome = AwsUtils::GetS3Client()->PutObject(object_request);
  if (!put_object_outcome.IsSuccess()) {
    auto error = put_object_outcome.GetError();
    upload_status_ = Status::NetworkError(error.GetExceptionName() + " : ", error.GetMessage());
    return;
  }
  LOG("Successfully uploaded {0} to S3. Sending SQS notification...", filename_);

  std::string s3_file_uri = "s3://" + AwsUtils::GetS3Bucket() + "/" + s3_key_name;
  Status sqs_notify_status = SendSQSNotification(s3_file_uri);
  if (!sqs_notify_status.ok()) {
    upload_status_ = sqs_notify_status;
    return;
  }
}

/*
void S3UploadTask::Execute() {
  
  LOG("Uploading file: {0}{1} to s3://{2}/{3}/{1}.", local_path_, file_prefix_, bucket_name_, s3_path_);
  for (auto& f : fs::directory_iterator(local_path_)) {
    std::string fq_file = f.path().string();
    size_t filename_pos = fq_file.find(file_prefix_);
    if (filename_pos != std::string::npos) {
      std::string filename = fq_file.substr(filename_pos);
      LOG("MATCH FILE: {0}", filename);

      std::string key_name = s3_path_ + "/" + filename;
      const std::shared_ptr<Aws::IOStream> input_data =
      Aws::MakeShared<Aws::FStream>("SampleAllocationTag",
          fq_file.c_str(),
          std::ios_base::in | std::ios_base::binary);

      Aws::S3::Model::PutObjectRequest object_request;

      object_request.SetBucket(bucket_name_);
      object_request.SetKey(key_name);
      object_request.SetBody(input_data);

      // Put the object
      auto put_object_outcome = owning_thread()->s3_client()->PutObject(object_request);
      if (!put_object_outcome.IsSuccess()) {
          auto error = put_object_outcome.GetError();
          LOG_ERROR("ERROR: {0}: {1}", error.GetExceptionName(), error.GetMessage());
          return;
      }
    }
  }
}
*/

} // namespace memcachedumper
