
set(DUMPER_SRCS
  dumper.cc
)

add_library(dumper ${DUMPER_SRCS})
target_link_libraries(dumper common spdlog::spdlog)
