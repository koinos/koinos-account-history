file(GLOB HEADERS "include/koinos/account_history/*.hpp")
add_library(koinos_account_history_lib
            account_history.cpp
            state.cpp
            ${HEADERS})
target_link_libraries(koinos_account_history_lib Koinos::state_db Koinos::exception Koinos::crypto Koinos::log Koinos::util Koinos::mq)
target_include_directories(koinos_account_history_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library(Koinos::account_history ALIAS koinos_account_history_lib)
