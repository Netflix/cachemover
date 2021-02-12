#include "common/logger.h"
#include "dumper/dumper.h"
#include "utils/status.h"
#include "extern/CLI/CLI.hpp"
#include <iostream>
#include <string>
#include <experimental/filesystem>

using std::string;
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

  std::string ip;
  int port;
  int num_threads;
  int bulk_get_threshold;
  uint64_t bufsize;
  uint64_t memlimit;
  uint64_t keyfilesize;
  uint64_t datafilesize;
  int only_expire_after;
  std::string log_file_path;
  std::string output_dir_path;
  bool checkpoint_resume = false;
  bool is_s3_dump = false;
  std::string s3_bucket;
  std::string s3_path;
  std::string req_id;
  std::string dest_ips_filepath;
  std::string all_ips_filepath;

  IGNORE_RET_VAL(app.add_option("-i,--ip,ip", ip,
      "Memcached IP.")->required());
  IGNORE_RET_VAL(app.add_option("-p,--port,port", port,
      "Memcached port.")->required());
  IGNORE_RET_VAL(app.add_option("-t,--threads,threads", num_threads,
      "Num. threads")->required());
  IGNORE_RET_VAL(app.add_option("-b,--bufsize,bufsize", bufsize,
      "Size of single memory buffer (in bytes).")->required());
  IGNORE_RET_VAL(app.add_option("-m,--memlimit,memlimit", memlimit,
      "Maximum allowable memory usage (in bytes).")->required());
  IGNORE_RET_VAL(app.add_option("-k,--key_file_size,key_file_size", keyfilesize,
      "The maximum size for each key file (in bytes).")->required());
  IGNORE_RET_VAL(app.add_option("-d,--data_file_size,data_file_size", datafilesize,
      "The maximum size for each date file (in bytes).")->required());
  IGNORE_RET_VAL(app.add_option("-l,--log_file_path,log_file_path", log_file_path,
      "Desired log file path.")->required());
  IGNORE_RET_VAL(app.add_option("-o,--output_dir,output_dir", output_dir_path,
      "Desired output directory path.")->required());
  IGNORE_RET_VAL(app.add_option("-g,--bulk_get_threshold,bulk_get_threshold",
      bulk_get_threshold, "Number of keys to bulk get."));
  IGNORE_RET_VAL(app.add_option("-e,--only_expire_after_s,only_expire_after_s",
      only_expire_after, "Only dump keys that expire after these many seconds."));
  IGNORE_RET_VAL(app.add_option("-c,--checkpoint_resume,checkpoint_resume",
      checkpoint_resume, "Resume dump from previous incomplete run."));
  IGNORE_RET_VAL(app.add_option("-s,--s3_dump",
      is_s3_dump, "Uploads dumped files to S3 if set."));
  IGNORE_RET_VAL(app.add_option("--s3_bucket",
      s3_bucket, "S3 Bucket name."));
  IGNORE_RET_VAL(app.add_option("--s3_path",
      s3_path, "S3 Final Path."));
  IGNORE_RET_VAL(app.add_option("-r, --req_id",
      req_id, "Dump ID assigned by requesting service.")->required());
  IGNORE_RET_VAL(app.add_option("--dest_ips_filepath", dest_ips_filepath,
      "Path to file containing one IP:port per line to narrow the dump to."));
  IGNORE_RET_VAL(app.add_option("--all_ips_filepath", all_ips_filepath,
      "Path to file containing one IP:port per line of all instances in the ASG."));
  CLI11_PARSE(app, argc, argv);

  opts.set_memcached_hostname(ip);
  opts.set_memcached_port(port);
  opts.set_num_threads(num_threads);
  opts.set_chunk_size(bufsize); // 1MB
  opts.set_max_memory_limit(memlimit); // 64MB
  opts.set_max_key_file_size(keyfilesize); // 1MB
  opts.set_max_data_file_size(datafilesize); // 1MB
  opts.set_log_file_path(log_file_path);
  opts.set_output_dir_path(output_dir_path);
  opts.set_bulk_get_threshold(bulk_get_threshold);
  opts.set_only_expire_after(only_expire_after);
  opts.set_resume_mode(checkpoint_resume);
  opts.set_is_s3_dump(is_s3_dump);
  if (is_s3_dump) {
    opts.set_s3_bucket_name(s3_bucket);
    opts.set_s3_final_path(s3_path);
  }
  opts.set_req_id(req_id);
  opts.set_dest_ips_filepath(dest_ips_filepath);
  opts.set_all_ips_filepath(all_ips_filepath);
  std::cout << "destips filepath: " << dest_ips_filepath << std::endl;

  if (is_s3_dump) {
    if (s3_bucket.empty() || s3_path.empty()) {
      LOG_ERROR("Configuration error: S3 Bucket and Path required if <is_s3_dump> is set to True.");
      exit(-1);
    }
  }
  if (num_threads * 2 * bufsize > memlimit) {
        LOG_ERROR("Configuration error: Memory given is not enough for all threads.\n\
            Given: {0} bytes. \
            Required: {1} bytes for {2} buffers of size {3} (2 buffers per thread)",
            memlimit, num_threads * 2 * bufsize, 2 * num_threads, bufsize);
        exit(-1);
  }

  return 0;
}

int main(int argc, char** argv) {

  // Init the AWS SDK.
  Aws::SDKOptions options;
  Aws::InitAPI(options);

  memcachedumper::DumperOptions opts;
  IGNORE_RET_VAL(ParseCLIOptions(argc, argv, opts));

  memcachedumper::DumperMain(opts);

  // Shutdown the AWS SDK.
  Aws::ShutdownAPI(options);

  LOG("Dumper exiting...");
  return 0;
}

