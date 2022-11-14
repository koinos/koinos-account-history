#pragma

#include <koinos/state_db/state_db.hpp>

#include <koinos/account_history/account_history.pb.h>
#include <koinos/rpc/account_history/account_history_rpc.pb.h>
#include <koinos/protocol/protocol.pb.h>
#include <koinos/broadcast/broadcast.pb.h>

namespace koinos::account_history {

using rpc::account_history::account_history_entry;

KOINOS_DECLARE_EXCEPTION( request_limit_exception );
KOINOS_DECLARE_EXCEPTION( unknown_record );

namespace detail { class account_history_impl; }

enum class fork_resolution_algorithm
{
   fifo,
   block_time,
   pob
};

class account_history
{
private:
   std::unique_ptr< detail::account_history_impl > _my;

public:
   account_history();
   account_history( const std::set< std::string >& whitelist );
   ~account_history();

   void open( const std::filesystem::path& p, fork_resolution_algorithm algo, bool reset );
   void close();

   void handle_block( const broadcast::block_accepted& );
   void handle_irreversible( const broadcast::block_irreversible& );

   rpc::account_history::get_account_history_response get_account_history( const rpc::account_history::get_account_history_request& ) const;

   uint64_t get_lib_height() const;
   uint64_t get_recent_entries_count();
};

} // koinos::account_history
