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
