#pragma once

#include "tasks/task.h"
#include "utils/key_value_writer.h"

#include <curl/curl.h>

#include <string>

namespace memcachedumper {

// Forward declares
class MetaBufferSlice;
class Socket;

class ProcessMetabufTask : public Task {
 public:
  ProcessMetabufTask(const std::string& filename, bool is_s3_dump);
  ~ProcessMetabufTask() = default;

  std::string UrlDecode(std::string& str);

  void ProcessMetaBuffer(MetaBufferSlice* mslice);

  void Execute() override;

 private:

  // Writes out 'filename_' to a thread specific checkpoint file to indicate that
  // we've already processed that keyfile.
  void MarkCheckpoint();

  std::string filename_;

  std::unique_ptr<KeyValueWriter> data_writer_;

  // CURL object used for decoding URL encoded keys.
  CURL* curl_;

  // Queues up an S3UploadTask on 'filename_' if true.
  bool is_s3_dump_;

};

} // namespace memcachedumper
