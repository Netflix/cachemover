
set(UTILS_SRCS
  mem_mgr.cc
  sockaddr.cc
  socket.cc
  socket_pool.cc
  status.cc
)

add_library(utils ${UTILS_SRCS})
target_link_libraries(utils spdlog::spdlog)
