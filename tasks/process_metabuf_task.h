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
  ~ProcessMetabufTask() = default;

  void ProcessMetaBuffer(MetaBufferSlice* mslice);

  void Execute() override;

 private:
  std::string filename_;

  std::unique_ptr<KeyValueWriter> data_writer_;
};

} // namespace memcachedumper
