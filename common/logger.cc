#include "common/logger.h"

#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace memcachedumper {

static Logger* global_logger_;

void Logger::InitGlobalLogger(
    const std::string& logger_name, const std::string& filepath) {
  global_logger_ = new Logger(logger_name, filepath);
  spdlog::set_default_logger(global_logger_->logger());
}

Logger::Logger(const std::string& logger_name, const std::string& filepath) {
  logger_ = spdlog::basic_logger_mt<spdlog::async_factory>(logger_name, filepath);
}

} // namespace memcachedumper
