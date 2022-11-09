file(GLOB HEADERS "include/*.hpp")

add_executable(koinos_account_history
               main.cpp
               ${HEADERS})
target_link_libraries(koinos_account_history PUBLIC Koinos::account_history Koinos::crypto Koinos::exception Koinos::proto Koinos::log Koinos::mq Koinos::git Boost::program_options yaml-cpp)
target_include_directories(koinos_account_history PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
install(TARGETS
   koinos_account_history
   RUNTIME DESTINATION bin
   LIBRARY DESTINATION lib
   ARCHIVE DESTINATION lib
)
