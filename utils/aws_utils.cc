#include "utils/aws_utils.h"
#include "utils/memcache_utils.h"

#include <iostream>
#include <sstream>

#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/CreateQueueResult.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/GetQueueUrlResult.h>

namespace memcachedumper {

// Static member declarations
std::string AwsUtils::s3_bucket_;
std::string AwsUtils::s3_path_;
std::string AwsUtils::sqs_url_;
Aws::S3::S3Client* AwsUtils::s3_client_;
Aws::SQS::SQSClient* AwsUtils::sqs_client_;

void AwsUtils::SetS3Bucket(std::string s3_bucket) {
  AwsUtils::s3_bucket_ = s3_bucket;
}

void AwsUtils::SetS3Path(std::string s3_path) {
  AwsUtils::s3_path_ = s3_path;
}
void AwsUtils::SetCachedSQSQueueURL(std::string sqs_url) {
  AwsUtils::sqs_url_ = sqs_url;
}

void AwsUtils::SetS3Client(Aws::S3::S3Client* s3_client) {
  AwsUtils::s3_client_ = s3_client;
}
void AwsUtils::SetSQSClient(Aws::SQS::SQSClient* sqs_client) {
  AwsUtils::sqs_client_ = sqs_client;
}

std::string AwsUtils::GetSQSQueueName() {
  return "native-cachewarmerapp-" + MemcachedUtils::GetReqId();
}

Status AwsUtils::GetSQSUrlFromName(std::string& queue_name, std::string* out_url) {
  Aws::SQS::Model::GetQueueUrlRequest gqu_req;
  gqu_req.SetQueueName(queue_name);

  auto gqu_out = AwsUtils::sqs_client_->GetQueueUrl(gqu_req);
  if (!gqu_out.IsSuccess()) {
    return Status::NetworkError("Could not get SQS queue URL for " + queue_name, gqu_out.GetError().GetMessage());
  }

  *out_url = gqu_out.GetResult().GetQueueUrl();
  return Status::OK();
}

Status AwsUtils::CreateNewSQSQueue(std::string& queue_name, std::string* out_url) {
  Aws::SQS::Model::CreateQueueRequest cq_req;
  cq_req.SetQueueName(queue_name);
  auto cq_out = AwsUtils::sqs_client_->CreateQueue(cq_req);
  if (!cq_out.IsSuccess()) {
    return Status::NetworkError("Error creating queue " + queue_name,
        cq_out.GetError().GetMessage());
  }
  *out_url = cq_out.GetResult().GetQueueUrl();
  return Status::OK();
}

} // namespace memcachedumper
