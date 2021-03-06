/**
 *
 *  Copyright 2021 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

#pragma once

#include <spdlog/spdlog.h>

#include <assert.h>

#define LOG(s, ...) do {   \
  spdlog::info(s, ##__VA_ARGS__);    \
  } while(0);

#define LOG_ERROR(s, ...) do {   \
  spdlog::error(s, ##__VA_ARGS__);    \
  } while(0);

namespace memcachedumper {

class Logger {
 public:
  static void InitGlobalLogger(
      const std::string& logger_name, const std::string& filepath);

 private:
  Logger(const std::string& logger_name, const std::string& filepath);
  std::shared_ptr<spdlog::logger> logger() { return logger_; }

  std::shared_ptr<spdlog::logger> logger_;
};

} // namespace memcachedumper
