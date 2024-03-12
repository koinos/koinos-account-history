add_library(account_history
  koinos/account_history/account_history.cpp
  koinos/account_history/state.cpp

  koinos/account_history/account_history.hpp
  koinos/account_history/state.hpp)

target_link_libraries(
  account_history
    PUBLIC
      Koinos::crypto
      Koinos::exception
      Koinos::log
      Koinos::state_db
      Koinos::util)

target_include_directories(
  account_history
    PUBLIC
      $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

koinos_add_format(TARGET account_history)

add_executable(koinos_account_history koinos_account_history.cpp)
target_link_libraries(
  koinos_account_history
    PRIVATE
      account_history
      Koinos::crypto
      Koinos::exception
      Koinos::git
      Koinos::log
      Koinos::mq
      Koinos::proto
      Boost::exception
      Boost::program_options
      yaml-cpp)

target_include_directories(
  koinos_account_history
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

koinos_add_format(TARGET koinos_account_history)

koinos_install(TARGETS koinos_account_history)
