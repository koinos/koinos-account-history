#pragma once

#include <koinos/chain/chain.pb.h>

#include <koinos/state_db/state_db_types.hpp>

namespace koinos {

namespace account_history::space {

const chain::object_space history_record();
const chain::object_space account_metadata();
const chain::object_space account_history();

} // account_history::space

} // koinos
