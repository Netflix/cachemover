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
  ProcessMetabufTask(const std::string& filename, int num_files);
  ~ProcessMetabufTask() = default;

  void ProcessMetaBuffer(MetaBufferSlice* mslice);

  void Execute() override;

 private:
  std::string filename_;

  // Index of the keyfile that we're processing.
  int keyfile_idx_;

  std::unique_ptr<KeyValueWriter> data_writer_;

  // CURL object used for decoding URL encoded keys.
  CURL* curl_;
};

} // namespace memcachedumper
