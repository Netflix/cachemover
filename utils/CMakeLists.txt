
set(UTILS_SRCS
  aws_utils.cc
  file_util.cc
  ketama_hash.cc
  key_filter.cc
  key_value_writer.cc
  mem_mgr.cc
  memcache_utils.cc
  metrics.cc
  net_util.cc
  sockaddr.cc
  socket.cc
  socket_pool.cc
  status.cc
)

add_library(utils ${UTILS_SRCS})
target_link_libraries(utils tasks spdlog::spdlog)
