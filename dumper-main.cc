#include "common/logger.h"
#include "dumper/dumper.h"
#include "utils/status.h"
#include "extern/CLI/CLI.hpp"
#include <iostream>
#include <string>

using std::string;
namespace memcachedumper {

void DumperMain(DumperOptions& opts) {

  // Initialize our global logger.
  Logger::InitGlobalLogger("global_logger", opts.log_file_path());

  // Set up the dumper with the provided options.
  Dumper dumper(opts);
  Status dumper_status = dumper.Init();
  if (!dumper_status.ok()) {
    std::cout << "Received fatal error: " << dumper_status.ToString() << std::endl;
    LOG_ERROR("Received fatal error: " + dumper_status.ToString());
    exit(-1);
  }

  // Begin dumping.
  dumper.Run();
}

} // namespace memcachedumper

int ParseCLIOptions(int argc, char **argv, memcachedumper::DumperOptions& opts) {

  CLI::App app("Memcached dumper options");

  std::string ip;
  int port;
  int num_threads;
  int bulk_get_threshold;
  uint64_t bufsize;
  uint64_t memlimit;
  uint64_t keyfilesize;
  uint64_t datafilesize;
  int only_expire_after;
  std::string output_dir_path;

  IGNORE_RET_VAL(app.add_option("-i,--ip,ip", ip,
      "Memcached IP."));
  IGNORE_RET_VAL(app.add_option("-p,--port,port", port,
      "Memcached port."));
  IGNORE_RET_VAL(app.add_option("-t,--threads,threads", num_threads,
      "Num. threads"));
  IGNORE_RET_VAL(app.add_option("-b,--bufsize,bufsize", bufsize,
      "Size of single memory buffer (in bytes)."));
  IGNORE_RET_VAL(app.add_option("-m,--memlimit,memlimit", memlimit,
      "Maximum allowable memory usage (in bytes)."));
  IGNORE_RET_VAL(app.add_option("-k,--key_file_size,key_file_size", keyfilesize,
      "The maximum size for each key file (in bytes)."));
  IGNORE_RET_VAL(app.add_option("-d,--data_file_size,data_file_size", datafilesize,
      "The maximum size for each date file (in bytes)."));
  IGNORE_RET_VAL(app.add_option("-o,--output_dir,output_dir", output_dir_path,
      "Desired output directory path."));
  IGNORE_RET_VAL(app.add_option("-g,--bulk_get_threshold,bulk_get_threshold",
      bulk_get_threshold, "Number of keys to bulk get."));
  IGNORE_RET_VAL(app.add_option("-e,--only_expire_after_s,only_expire_after_s",
      only_expire_after, "Only dump keys that expire after these many seconds."));

  CLI11_PARSE(app, argc, argv);

  opts.set_memcached_hostname(ip);
  opts.set_memcached_port(port);
  opts.set_num_threads(num_threads);
  opts.set_chunk_size(bufsize); // 1MB
  opts.set_max_memory_limit(memlimit); // 64MB
  opts.set_max_key_file_size(keyfilesize); // 1MB
  opts.set_max_data_file_size(datafilesize); // 1MB
  opts.set_log_file_path("logfile.txt");
  opts.set_output_dir_path(output_dir_path);
  opts.set_bulk_get_threshold(bulk_get_threshold);
  opts.set_only_expire_after(only_expire_after);

  if (num_threads * 2 * bufsize > memlimit) {
        std::cout << "Total memory provisioned is not enough for the total threads and buffers !" << std::endl;
        LOG_ERROR("Received fatal error: Total memory provisioned is not enough for the total threads and buffers ! ");
        exit(-1);
  }

  return 0;
}

int main(int argc, char** argv) {

  memcachedumper::DumperOptions opts;
  IGNORE_RET_VAL(ParseCLIOptions(argc, argv, opts));

  memcachedumper::DumperMain(opts);

  LOG("Exiting program!");
  return 0;
}

