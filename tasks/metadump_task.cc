#include "tasks/metadump_task.h"

#include "common/logger.h"
#include "tasks/process_metabuf_task.h"
#include "tasks/task_scheduler.h"
#include "tasks/task_thread.h"
#include "utils/mem_mgr.h"
#include "utils/metrics.h"
#include "utils/socket.h"

#include <string.h>

#include <string>
#include <fstream>
#include <memory>
#include <sstream>
#include <random>


#define METADUMP_END_STR "END\r\n"
#define METADUMP_BUSY_STR "BUSY currently processing crawler request\r\n"
#define METADUMP_END_STRLEN 5
#define METADUMP_BUSY_STRLEN 43

namespace memcachedumper {

//TODO: Clean up code.

MetadumpTask::MetadumpTask(int slab_class, const std::string& file_path,
    uint64_t max_file_size, MemoryManager *mem_mgr, bool is_s3_dump)
  : slab_class_(slab_class),
    file_path_(file_path),
    file_prefix_(MemcachedUtils::KeyFilePrefix()),
    max_file_size_(max_file_size),
    mem_mgr_(mem_mgr),
    is_s3_dump_(is_s3_dump) {
}

void WriteCompleteMarker(int num_files) {
  std::ofstream complete_marker_file;
  complete_marker_file.open(MemcachedUtils::GetKeyFilePath() + "/ALL_KEYFILES_DUMPED");
  std::string info_str = std::to_string(num_files) + " key files dumped.\n";
  complete_marker_file.write(info_str.c_str(), info_str.length());
  complete_marker_file.close();
}

void MetadumpTask::Execute() {

  memcached_socket_ = owning_thread()->task_scheduler()->GetMemcachedSocket();
  assert(memcached_socket_ != nullptr);

  bool busy_crawler = true;
  std::random_device rand_device;
  std::mt19937 rand_generator(rand_device());
  std::uniform_int_distribution<> uniform_distribution(3, 19);

  while (busy_crawler) {

    busy_crawler = false;
    std::string metadump_cmd("lru_crawler metadump all\n");
    Status send_status = SendCommand(metadump_cmd);
    if (!send_status.ok()) {
      LOG_ERROR("SendCommand() failed. (Status: {0})", send_status.ToString());
      abort();
    }

    Status stat = RecvResponse();

    if (stat.IsBusyLRUCrawler()) {
      int sleep_duration_s = uniform_distribution(rand_generator);
      LOG_ERROR("LRU crawler is busy. Retrying after {0} seconds.", sleep_duration_s);
      sleep(sleep_duration_s);
      busy_crawler = true;
      continue;
    } else if (stat.IsNetworkError()) {
      // This is a temporary way to deal with EAGAIN.
      // TODO: This was done as a quick patch. Clean up later.
      int sleep_duration_s = uniform_distribution(rand_generator);
      LOG_ERROR("Memcached not responding. Retrying after {0} seconds.", sleep_duration_s);
      sleep(sleep_duration_s);
      // Treat as busy crawler for now.
      busy_crawler = true;
    } else if (!stat.ok()) {
      LOG_ERROR("RecvResponse() failed. (Status: {0})", stat.ToString());
      assert(false);
    }

  }

  // Return socket.
  owning_thread()->task_scheduler()->ReleaseMemcachedSocket(memcached_socket_);
  memcached_socket_ = nullptr;
}


// TODO: Move into utils
Status MetadumpTask::SendCommand(const std::string& metadump_cmd) {
  int32_t bytes_sent;

  RETURN_ON_ERROR(memcached_socket_->Send(
      reinterpret_cast<const uint8_t*>(metadump_cmd.c_str()),
      metadump_cmd.length(), &bytes_sent));

  return Status::OK();
}

Status MetadumpTask::RecvResponse() {

  LOG("Beginning to receive metadump...");
  uint8_t *buf = mem_mgr_->GetBuffer();
  assert(buf != nullptr);
  uint64_t chunk_size = mem_mgr_->chunk_size();
  int32_t bytes_read = 0;
  uint64_t bytes_written_to_file = 0;
  int num_files = 0;
  std::ofstream chunk_file;
  std::string chunk_file_name(file_path_ + file_prefix_ + std::to_string(num_files));
  chunk_file.open(chunk_file_name);

  bool reached_end = false;
  bool busy_crawler = false;

  do {
    Status stat = memcached_socket_->Recv(buf, chunk_size-1, &bytes_read);
    if (!stat.ok()) {
      chunk_file.close();
      mem_mgr_->ReturnBuffer(buf);
      return stat;
    }

    uint8_t *unwritten_tail = nullptr;
    size_t bytes_to_write = bytes_read;

    busy_crawler = (strncmp(reinterpret_cast<const char*>(&buf[bytes_read - METADUMP_BUSY_STRLEN]),
            METADUMP_BUSY_STR, METADUMP_BUSY_STRLEN) == 0) ? true:false;


    if (busy_crawler) {
      chunk_file.close();
      mem_mgr_->ReturnBuffer(buf);
      return Status::BusyLRUCrawler("LRU crawler is busy");
    }

    reached_end =
        !strncmp(reinterpret_cast<const char*>(&buf[bytes_read - METADUMP_END_STRLEN]),
            METADUMP_END_STR, METADUMP_END_STRLEN);

    bool rotate_file = false;
    if ((bytes_written_to_file + bytes_read >= max_file_size_) && !reached_end) {
      // TODO: Make more efficient. Avoid the copy.
      std::string str_representation(reinterpret_cast<char*>(buf), bytes_read);
      size_t last_key_pos = str_representation.rfind("key=", bytes_read);

      // We want to make sure to have every file end at a newline boundary.
      // So, find the last "key=" string in the buffer and write upto but not including
      // that.
      bytes_to_write = last_key_pos;

      // Save the position from the last "key=" onwards to write to the next file.
      unwritten_tail = buf + last_key_pos;
      rotate_file = true;
    }

    chunk_file.write(reinterpret_cast<char*>(buf), bytes_to_write);
    bytes_written_to_file += bytes_to_write;

    if ((bytes_written_to_file >= max_file_size_ || rotate_file) && !reached_end) {
      // Chunk the file.
      chunk_file.close();
      chunk_file.clear();

      DumpMetrics::update_total_keys(FileUtils::CountNumLines(chunk_file_name));

      ProcessMetabufTask *ptask = new ProcessMetabufTask(
          file_path_ + file_prefix_ + std::to_string(num_files), is_s3_dump_);

      TaskScheduler *task_scheduler = owning_thread()->task_scheduler();
      task_scheduler->SubmitTask(ptask);

      num_files++;
      {
        std::string temp_str = file_path_ + file_prefix_ + std::to_string(num_files);
        chunk_file_name.replace(0, temp_str.length(), temp_str);
      }
      chunk_file.open(chunk_file_name);
      bytes_written_to_file = 0;

      // Write the last partial line if it exists.
      if (unwritten_tail) {
        size_t remaining_bytes = bytes_read - bytes_to_write;
        chunk_file.write(
            reinterpret_cast<char*>(unwritten_tail), remaining_bytes);
        bytes_written_to_file = remaining_bytes;
      }
    }
  } while (bytes_read != 0 && !reached_end);

  chunk_file.close();

  // Subtract 1 to discount the "END\r\n" line.
  DumpMetrics::update_total_keys(FileUtils::CountNumLines(chunk_file_name) - 1);

  ProcessMetabufTask *ptask = new ProcessMetabufTask(
      file_path_ + file_prefix_ + std::to_string(num_files), is_s3_dump_);

  TaskScheduler *task_scheduler = owning_thread()->task_scheduler();
  task_scheduler->SubmitTask(ptask);

  num_files++;

  mem_mgr_->ReturnBuffer(buf);

  WriteCompleteMarker(num_files);
  return Status::OK();
}

} // namespace memcachedumper
