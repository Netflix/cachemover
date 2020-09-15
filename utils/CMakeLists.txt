
set(UTILS_SRCS
  file_util.cc
  key_value_writer.cc
  mem_mgr.cc
  memcache_utils.cc
  net_util.cc
  ring_buffer.cc
  sockaddr.cc
  socket.cc
  socket_pool.cc
  status.cc
)

add_library(utils ${UTILS_SRCS})
target_link_libraries(utils tasks spdlog::spdlog)
