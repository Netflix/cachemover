
set(TASKS_SRCS
  metadump_task.cc
  process_metabuf_task.cc
  resume_task.cc
  s3_upload_task.cc
  task.cc
  task_thread.cc
  task_scheduler.cc
)

add_library(tasks ${TASKS_SRCS})
target_link_libraries(tasks utils spdlog::spdlog ${AWSSDK_LINK_LIBRARIES})
