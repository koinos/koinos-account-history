file(GLOB HEADERS "include/koinos/account_history/*.hpp")
add_library(koinos_account_history_lib
            controller.cpp
            chronicler.cpp
            execution_context.cpp
            host_api.cpp
            indexer.cpp
            proto_utils.cpp
            session.cpp
            system_calls.cpp
            thunk_dispatcher.cpp
            resource_meter.cpp
            state.cpp
            ${HEADERS})
target_link_libraries(koinos_account_history_lib Koinos::state_db Koinos::exception Koinos::crypto Koinos::log Koinos::util Koinos::mq)
target_include_directories(koinos_account_history_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library(Koinos::account_history ALIAS koinos_account_history_lib)
