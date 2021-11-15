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

#include "common/logger.h"

#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace memcachedumper {

static Logger* global_logger_;

void Logger::InitGlobalLogger(
    const std::string& logger_name, const std::string& filepath) {
  global_logger_ = new Logger(logger_name, filepath);
  spdlog::set_default_logger(global_logger_->logger());
  global_logger_->logger()->flush_on(spdlog::level::info);
  spdlog::set_pattern("[%^%D %H:%M:%S%$][%t][%L]  %v");
}

Logger::Logger(const std::string& logger_name, const std::string& filepath) {
  logger_ = spdlog::basic_logger_mt(logger_name, filepath);
}

} // namespace memcachedumper
