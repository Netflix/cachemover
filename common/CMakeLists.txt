
set(COMMON_SRCS
  logger.cc
  rest_endpoint.cc
)

add_library(common ${COMMON_SRCS})
target_link_libraries(common spdlog::spdlog ${PISTACHE_LIBRARY})
