#include "tasks/metadump_task.h"

#include "tasks/task_scheduler.h"
#include "tasks/task_thread.h"
#include "utils/mem_mgr.h"
#include "utils/socket.h"

#include <string.h>

#include <fstream>
#include <memory>

namespace memcachedumper {


//TODO: Clean up code.

MetadumpTask::MetadumpTask(Socket *socket, int slab_class, const std::string& file_prefix,
    uint64_t max_file_size, MemoryManager *mem_mgr)
  : memcached_socket_(socket),
    slab_class_(slab_class),
    file_prefix_(file_prefix),
    max_file_size_(max_file_size),
    mem_mgr_(mem_mgr) {
}

void MetadumpTask::Execute() {

  // TODO: Check slab class
  std::string metadump_cmd("lru_crawler metadump all\n");
  SendCommand(metadump_cmd);

  RecvResponse();
}


// TODO: Move into utils
int MetadumpTask::SendCommand(const std::string& metadump_cmd) {
  int bytes_sent;

  bytes_sent = memcached_socket_->Send(
      reinterpret_cast<const uint8_t*>(metadump_cmd.c_str()), metadump_cmd.length());
  if (bytes_sent < 0) {
    std::cout << "Failed to send keydump command" << std::endl;
    return -1;
  }

  return 0;
}

int MetadumpTask::RecvResponse() {

  uint8_t *buf = mem_mgr_->GetBuffer();
  uint64_t chunk_size = mem_mgr_->chunk_size();
  uint64_t bytes_read = 0;
  uint64_t bytes_written_to_file = 0;
  int num_files = 0;
  std::ofstream chunk_file;
  std::string chunk_file_name(file_prefix_ + std::to_string(num_files));
  chunk_file.open(file_prefix_ + std::to_string(num_files));
  do {
    bytes_read = memcached_socket_->Recv(buf, chunk_size-1);
    if (bytes_read < 0) {
      std::cout << "Failed to recv" << std::endl;
      exit(0);
    }

    chunk_file.write(reinterpret_cast<char*>(buf), bytes_read);
    buf[bytes_read] = '\0';
    bytes_written_to_file += bytes_read;
    if (strncmp(reinterpret_cast<const char*>(&buf[bytes_read - 5]), "END\r\n", 5) == 0) {
      break;
    }

    if (bytes_written_to_file > max_file_size_) {
      // Chunk the file.
      chunk_file.close();
      chunk_file.clear();
      // Start a task to print contents of the file.
      // TODO: This is only for testing the framework. Change later on.
      PrintKeysFromFileTask *ptask = new PrintKeysFromFileTask(
          file_prefix_ + std::to_string(num_files), mem_mgr_);

      TaskScheduler *task_scheduler = owning_thread()->task_scheduler();
      task_scheduler->SubmitTask(ptask);

      num_files++;
      {
        std::string temp_str = file_prefix_ + std::to_string(num_files);
        chunk_file_name.replace(0, temp_str.length(), temp_str);
      }
      chunk_file.open(file_prefix_ + std::to_string(num_files));
      bytes_written_to_file = 0;

    }

  } while (bytes_read != 0);

  chunk_file.close();

  mem_mgr_->ReturnBuffer(buf);
  return 0;
}

} // namespace memcachedumper
