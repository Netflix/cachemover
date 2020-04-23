#include "common/logger.h"
#include "dumper/dumper.h"
#include "tasks/process_metabuf_task.h"
#include "tasks/task_scheduler.h"
#include "tasks/task_thread.h"
#include "utils/mem_mgr.h"
#include "utils/memcache_utils.h"
#include "utils/socket.h"

#include <fstream>

namespace memcachedumper {

ProcessMetabufTask::ProcessMetabufTask(const std::string& filename)
  : filename_(filename) {
}

void ProcessMetabufTask::ProcessMetaBuffer(MetaBufferSlice* mslice) {

  char* newline_pos = nullptr;
  do {

    const char *key_pos = mslice->next_key_pos();
    if (key_pos == nullptr) {
      //printf("Going to copy remaining to start. Newline_pos: %s   \n", newline_pos);
      if (newline_pos != nullptr) {
        mslice->CopyRemainingToStart(newline_pos);
      }
      break;
    }

    const char *exp_pos = mslice->next_exp_pos();
    if (exp_pos == nullptr) {
      //printf("Going to copy remaining to start. Newline_pos: %s   \n", newline_pos);
      if (newline_pos != nullptr) {
        mslice->CopyRemainingToStart(newline_pos);
      }
      break;
    }
    //printf("[%s]KEY: %.*s   ", owning_thread()->thread_name().c_str(), static_cast<int>(exp_pos - key_pos), key_pos);

    const char *la_pos = mslice->next_la_pos();
    if (la_pos == nullptr) {
      //printf("Going to copy remaining to start. Newline_pos: %s   \n", newline_pos);
      if (newline_pos != nullptr) {
        mslice->CopyRemainingToStart(newline_pos);
      }
      break;
    }

    char *unused;
    int32_t expiry = strtol(exp_pos + 4, &unused, 10);
    //char *curl_easy_unescape( CURL *curl, const char *url , int inlength, int *outlength );

    printf("ENCODED KEY: %.*s\n", static_cast<int>(exp_pos - key_pos - 4 - 1), const_cast<char*>(key_pos) + 4);
    int decoded_keylen = 0;
    char* decoded_key = curl_easy_unescape(curl_, const_cast<char*>(key_pos) + 4, static_cast<int>(exp_pos - key_pos - 4 - 1), &decoded_keylen);
    if (decoded_key == NULL) {
      std::cout << "Could not decode key! " << std::endl;
      abort();
    }
    printf("DECODED KEY: %s\n\n", decoded_key);
    //McData *new_key = new McData(const_cast<char*>(key_pos) + 4, static_cast<int>(exp_pos - key_pos - 4 - 1), expiry);
    McData *new_key = new McData(decoded_key, static_cast<size_t>(decoded_keylen), expiry);
    curl_free(decoded_key);
    data_writer_->ProcessKey(new_key);
/*
    //printf("New key: %s | %d\n", new_key->key().c_str(), new_key->expiry());
    char key_cmd_temp[300] = "get ";
    strncpy(key_cmd_temp + 4, key_pos + 4, exp_pos - key_pos - 4);
    strncpy(key_cmd_temp + 4 + (exp_pos - key_pos - 4), "\n", 1);
    //printf("CMD: %.*s", static_cast<int>(exp_pos - key_pos - 4) + 5, key_cmd_temp);

    int32_t nsent = 0;
    mc_sock->Send(reinterpret_cast<const uint8_t*>(key_cmd_temp), exp_pos - key_pos - 4 + 5, &nsent);

    //printf("NSENT: %d  ", nsent);

    char temp_buf[500];
    int32_t nread = 0;
    mc_sock->Recv(reinterpret_cast<uint8_t*>(&temp_buf), 499, &nread);
    temp_buf[nread] = '\0';
    //printf("Value: %s\n", temp_buf);
    //printf("EXP: %.*s\n", static_cast<int>(la_pos - exp_pos), exp_pos);
*/
    //data_writer_->PrintKeys();
  } while ((newline_pos = const_cast<char*>(mslice->next_newline())) != nullptr);

}

void ProcessMetabufTask::Execute() {

  /*if (!(strcmp(filename_.c_str(), "test_prefix11") == 0)) {
	  if (strcmp(filename_.c_str(), "test_prefix0") == 0) sleep(5);
	  std::cout <<" Not 10. Returning... Got: " << filename_.c_str() << std::endl;
	  return;
  }*/

  curl_ = curl_easy_init();
  if (curl_ == NULL) {
    std::cout << "Could not initialize CURL library; exiting..." << std::endl;
    abort();
  }

  Socket *mc_sock = owning_thread()->task_scheduler()->GetMemcachedSocket();
  assert(mc_sock != nullptr);

  uint8_t* data_writer_buf = owning_thread()->mem_mgr()->GetBuffer();
  assert(data_writer_buf != nullptr);
  data_writer_.reset(new KeyValueWriter(std::string("DATAFILE_" + filename_),
      owning_thread()->thread_name(),
      data_writer_buf, owning_thread()->mem_mgr()->chunk_size(),
      owning_thread()->task_scheduler()->dumper()->max_data_file_size(), mc_sock));

  // TODO: Check return status
  Status init_status = data_writer_->Init();
  if (!init_status.ok()) {
    LOG_ERROR("FAILED TO INITIALIZE KeyValueWriter");
    return;
  }

  std::ifstream metafile;
  metafile.open(filename_);

  LOG("Starting PrintKeysFromFileTask: " + filename_);

  char *metabuf = reinterpret_cast<char*>(owning_thread()->mem_mgr()->GetBuffer());
  uint64_t buf_size = owning_thread()->mem_mgr()->chunk_size();

  int32_t bytes_read = metafile.readsome(metabuf, buf_size);

  size_t mslice_size = bytes_read;
  char *buf_begin_fill = metabuf;
  while (bytes_read > 0) {

    MetaBufferSlice mslice(metabuf, mslice_size);

    ProcessMetaBuffer(&mslice);

    // Find number of bytes free in the buffer. Usually should be the entire 'buf_size',
    // but in some cases, we can have a little leftover unparsed bytes at the end of the
    // buffer which 'ProcessMetaBuffer()' would have copied to the start of the buffer.
    size_t free_bytes = mslice.free_bytes();
    size_t size_remaining = mslice.size() - free_bytes;

    // Start filling the buffer from after the unparsed bytes.
    buf_begin_fill = mslice.buf_begin_fill();
    bytes_read = metafile.readsome(buf_begin_fill, free_bytes);

    mslice_size = size_remaining + bytes_read;
  }

  data_writer_->Finalize();

  owning_thread()->account_keys_processed(data_writer_->num_processed_keys());
  metafile.close();

  owning_thread()->task_scheduler()->ReleaseMemcachedSocket(mc_sock);
  owning_thread()->mem_mgr()->ReturnBuffer(reinterpret_cast<uint8_t*>(metabuf));
  owning_thread()->mem_mgr()->ReturnBuffer(data_writer_buf);

  owning_thread()->PrintNumKeysProcessed();
}

} // namespace memcachedumper
