#include "dumper/dumper.h"

#include <iostream>
#include <string>

using std::string;
namespace memcachedumper {

void DumperMain(DumperOptions& opts) {
  Dumper dumper(opts);
  if (dumper.Init() < 0) {
    exit(-1);
  }

  dumper.Run();
}

} // namespace memcachedumper


int main(int argc, char** argv) {

  // TODO: Take command line arguments for the DumperOptions.

  memcachedumper::DumperOptions dummy_options;
  dummy_options.set_hostname("127.0.0.1");
  dummy_options.set_port(11211);
  dummy_options.set_num_threads(4);
  dummy_options.set_chunk_size(1048576); // 1MB
  dummy_options.set_max_memory_limit(67108864); // 64MB

  memcachedumper::DumperMain(dummy_options);

  std::cout << "Exiting program!" << std::endl;
  return 0;
}

