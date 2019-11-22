
set(TASKS_SRCS
  metadump_task.cc
  task.cc
  task_thread.cc
  task_scheduler.cc
)

add_library(tasks ${TASKS_SRCS})
