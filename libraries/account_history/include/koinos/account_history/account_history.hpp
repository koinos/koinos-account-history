#pragma

#include <koinos/state_db/state_db.hpp>

#include <koinos/account_history/account_history.pb.h>
#include <koinos/rpc/account_history/account_history_rpc.pb.h>
#include <koinos/protocol/protocol.pb.h>
#include <koinos/broadcast/broadcast.pb.h>

namespace koinos::account_history {

using rpc::account_history::account_history_entry;

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
   account_history( const std::vector< std::string >& whitelist );
   ~account_history();

   void open( const std::filesystem::path& p, fork_resolution_algorithm algo, bool reset );
   void close();

   void handle_block( const broadcast::block_accepted& );
   void handle_irreversible( const broadcast::block_irreversible& );

   std::vector< account_history_entry > get_account_history( const std::string& address, uint32_t seq_num, uint32_t limit, bool ascending, bool from_lib ) const;
};

} // koinos::account_history
