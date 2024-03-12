if (NOT BUILD_TESTING)
  return()
endif()

include(CTest)

koinos_add_test(
  account_history_tests
    SOURCES
      main.cpp
      account_history_tests.cpp)

target_link_libraries(
  account_history_tests
    PRIVATE
      account_history
      Koinos::crypto
      Koinos::exception
      Koinos::log
      Koinos::proto
      Koinos::state_db
      Koinos::util)

target_include_directories(
  account_history_tests
    PUBLIC
    ${PROJECT_SOURCE_DIR}/src # private headers
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>  # <prefix>/include
)

koinos_add_format(TARGET account_history_tests)

koinos_coverage(
  EXECUTABLE
    account_history_tests
  EXCLUDE
    "tests/*")
