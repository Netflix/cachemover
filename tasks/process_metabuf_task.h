#pragma once

#include "tasks/task.h"
#include "utils/key_value_writer.h"

#include <string>

namespace memcachedumper {

// Forward declares
class MetaBufferSlice;
class Socket;

class ProcessMetabufTask : public Task {
 public:
  ProcessMetabufTask(const std::string& filename);

  void ProcessMetaBuffer(MetaBufferSlice* mslice, Socket* mc_sock);

  void Execute() override;

 private:
  std::string filename_;

  std::unique_ptr<KeyValueWriter> data_writer_;
};

} // namespace memcachedumper
