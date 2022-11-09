#pragma

#include <koinos/crypto/multihash.hpp>
#include <koinos/exception.hpp>
#include <koinos/protocol/protocol.pb.h>
#include <koinos/broadcast/broadcast.pb.h>

namespace koinos::account_history {

namespace detail {

class account_history_impl;

} // detail

class account_history
{
private:
   std::unique_ptr< detail::account_history_impl > _my;

public:
   account_history( std::vector< std::string >& whitelist );
   ~account_history();

   void open( const std::filesystem::path& p, fork_resolution_algorithm algo, bool reset );
   void close();

   void handle_block( const broadcast::block_accepted& );
   void handle_irreversible( const broadcast::block_irreversible& );

   //std::vector< >get_account_history( const std::string& account, uint32_t seq_num, uint32_t limit, bool ascending ) const;
}

} // koinos::account_history
