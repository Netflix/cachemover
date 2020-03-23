#pragma once

#include <spdlog/spdlog.h>

#include <assert.h>


#define LOG(s) do {   \
  spdlog::info(s);    \
  } while(0);

#define LOG_ERROR(s) do {   \
  spdlog::error(s);    \
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
