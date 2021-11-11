// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
