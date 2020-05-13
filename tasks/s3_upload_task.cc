#include "tasks/s3_upload_task.h"

#include "common/logger.h"
#include "tasks/task_scheduler.h"
#include "tasks/task_thread.h"
#include "utils/mem_mgr.h"
#include "utils/memcache_utils.h"
#include "utils/socket.h"

#include <unistd.h>
#include <string.h>

#include <experimental/filesystem>
#include <fstream>
#include <sstream>

#include <aws/s3/model/PutObjectRequest.h>

namespace fs = std::experimental::filesystem;
namespace memcachedumper {

S3UploadTask::~S3UploadTask() = default;

S3UploadTask::S3UploadTask(std::string bucket_name, std::string path, std::string file_prefix)
  : bucket_name_(bucket_name),
    path_(path),
    file_prefix_(file_prefix) {
}

void S3UploadTask::Execute() {
  
  std::cout << "Uploading file: " << path_ << file_prefix_ << "to " << bucket_name_ << "/" << file_prefix_ << std::endl;
  for (auto& f : fs::directory_iterator(path_)) {
      //std::cout << "FILE: " << f.path().string() << std::endl;
    std::string fq_file = f.path().string();
    size_t filename_pos = fq_file.find(file_prefix_);
    if (filename_pos != std::string::npos) {
      std::string filename = fq_file.substr(filename_pos);
      std::cout << "MATCH FILE: " << filename << std::endl;

      std::string key_name = "native_dumper_upload_test/" + filename;
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
          std::cout << "ERROR: " << error.GetExceptionName() << ": "
        << error.GetMessage() << std::endl;
          return;
      }
    }
  }

/*
 for (auto& f : fs::directory_iterator("/mnt/sail_dumper_test/run_8")) {
   std::cout << f.path().string() << std::endl;
   std::string file_name = f.path().string();
   std::string key_name = "native_dumper_test/run_1/" + std::to_string(file_idx);
   std::cout << file_name << std::endl << key_name << std::endl;

   const std::shared_ptr<Aws::IOStream> input_data =
     Aws::MakeShared<Aws::FStream>("SampleAllocationTag",
           file_name.c_str(),
           std::ios_base::in | std::ios_base::binary);

   Aws::S3::Model::PutObjectRequest object_request;
   std::string bucket_name("evcache-test");

   object_request.SetBucket(bucket_name);
   object_request.SetKey("native_dumper_test/run_1/" + f.path().string());
   object_request.SetBody(input_data);

   // Put the object
   auto put_object_outcome = s3_client.PutObject(object_request);
   if (!put_object_outcome.IsSuccess()) {
       auto error = put_object_outcome.GetError();
       std::cout << "ERROR: " << error.GetExceptionName() << ": "
     << error.GetMessage() << std::endl;
       return false;
   }
  */
}


} // namespace memcachedumper
