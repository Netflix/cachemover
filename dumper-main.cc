#include "common/logger.h"
#include "dumper/dumper_config.h"
#include "dumper/dumper.h"
#include "utils/status.h"
#include "extern/CLI/CLI.hpp"
#include <iostream>
#include <string>
#include <experimental/filesystem>

namespace memcachedumper {

void DumperMain(DumperOptions& opts) {

  // Initialize our global logger.
  Logger::InitGlobalLogger("logger", opts.log_file_path());

  // Set up the dumper with the provided options.
  Dumper dumper(opts);
  Status dumper_status = dumper.Init();
  if (!dumper_status.ok()) {
    LOG_ERROR("Received fatal error: " + dumper_status.ToString());
    exit(-1);
  }

  // Begin dumping.
  dumper.Run();
}

} // namespace memcachedumper

int ParseCLIOptions(int argc, char **argv, memcachedumper::DumperOptions& opts) {

  CLI::App app("Memcached dumper options");

  std::string config_file_path;
  IGNORE_RET_VAL(app.add_option("--config_file_path", config_file_path,
        "Configuration file path.")->required());

  CLI11_PARSE(app, argc, argv);

  opts.set_config_file_path(config_file_path);
  return 0;
}

int main(int argc, char** argv) {

  // Init the AWS SDK.
  Aws::SDKOptions options;
  Aws::InitAPI(options);

  memcachedumper::DumperOptions opts;
  IGNORE_RET_VAL(ParseCLIOptions(argc, argv, opts));
  memcachedumper::Status s =
      memcachedumper::DumperConfig::LoadConfig(opts.config_file_path(), opts);
  if (!s.ok()) {
    LOG_ERROR(s.ToString());
    exit(-1);
  }

  memcachedumper::DumperMain(opts);

  // Shutdown the AWS SDK.
  Aws::ShutdownAPI(options);

  LOG("Dumper exiting...");
  return 0;
}

