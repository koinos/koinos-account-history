#include <koinos/account_history/account_history.hpp>

#include <koinos/state_db/state_db.hpp>

#include <koinos/account_history/account_history.pb.h>

namespace koinos::account_history {

namespace space
{
   const std::string record_space = "\x01";
   const std::string account_metadata_space = "\x02";
   const std::string account_history = "\x03";
}

namespace detail {

class account_history_impl {
   public:
      account_history_impl() = delete;
      account_history_impl( const std::vector< std::string >& );
      ~account_history_impl() = default;

      void open( const std::filesystem::path& p, fork_resolution_algorithm algo, bool reset );
      void close();

      void handle_block( const broadcast::block_accepted& );
      void handle_irreversible( const broadcast::block_irreversible& );

      void add_transaction( state_node_ptr state_node, shared_lock_ptr db_lock, const protocol::transaction& );
      void record_history( state_node_ptr state_node, shared_lock_ptr db_lock, const std::string& address, const std::string& trx_id );

   private:
      state_db::database _db;
      const std::vector< std::string >& _whitelist;
}

account_history_impl::account_history_impl( const std::vector< std::string >& whitelist ) :
   _whitelist( whitelist )
{}

void account_history_impl::open( const std::filesystem::path& p, fork_resolution_algorithm algo, bool reset )
{
   state_db::state_node_comparator_function comp;

   switch( algo )
   {
      case fork_resolution_algorithm::block_time:
         comp = &state_db::block_time_comparator;
         break;
      case fork_resolution_algorithm::pob:
         comp = &state_db::pob_comparator;
         break;
      case fork_resolution_algorithm::fifo:
         [[fallthrough]];
      default:
         comp = &state_db::fifo_comparator;
   }

   _db.open( p, []( state_db::state_node_ptr ){}, comp, _db.get_unique_lock() );

   if ( reset )
   {
      LOG(info) << "Resetting database...";
      _db.reset( _db.get_unique_lock() );
   }

   auto head = _db.get_head( _db.get_shared_lock() );
   LOG(info) << "Opened database at block - Height: " << head->revision() << ", ID: " << head->id();
}

void account_history_impl::close()
{
   _db.close( _db.get_unique_lock() );
}

void account_history_impl::handle_irreversible( const broadcast::block_irreversible& bim )
{
   auto db_lock = _db.get_unique_lock();
   auto lib = _db.get_node( bim.topology().id(), db_lock );

   if ( lib )
   {
      _db.commit_node( bim.topology().id(), db_lock );
   }
}

void account_history_impl::handle_block( const broadcast::block_accepted& bam )
{
   auto db_lock = _db.get_shared_lock();
   auto state_node = _db.create_writable_node( bam.block().header().previous(), bam.block().id(), bam.block.header(), db_lock );

   if ( !state_node )
      return;

   try
   {
      std::set< std::string > impacted_addresses;

      impacted_addresses.insert( bam.block().header().producer() );

      for ( const auto& event : bam.receipt().events() )
      {
         impacted_addresses.insert( source );
         impacted_addresses.insert( std::begin( event.impacted ), std::end( event.impacted ) );
      }

      for ( const auto& address : impacted_addressed )
      {
         record_history( state_node, address, bam.block.id() )
      }

      // Add block
      history_record record;
      record.mutable_block()->set_header( bam.block().header() );
      record.mutable_block()->set_receipt( bam.receipt() );

      state_node.put_object( space::transaction_space, trx.id(), util::converter::as< std::string >( record ) );

      // Add records for all contained transactions
      for ( std::size_t i = 0; i < bam.block().transactions().size(); i++ )
      {
         add_transaction( state_node, bam.block()transactions( i ), bam.receipt().transaction_receipts( i ) );
      }
   }
   catch ( ... )
   {
      _db.discard_node( bam.block.id(), db_lock );
      throw;
   }

   _db.finalize_node( bam.block.id() );
}

void account_history_impl::add_transaction( state_node_ptr state_node, const protocol::transaction& trx, const protocol::transaction_receipt& trx_rec )
{
   std::set< std::string > impacted_addresses;

   impacted_addresses.insert( trx.header().payer() )

   if ( trx.header().payee().size() )
      impacted_addresses.insert( trx.header().payee() )

   for ( const auto& op : trx.operations )
   {
      if ( o.has_upload_contract() )
         impacted_addresses.insert( o.upload_contract().contract_id() )
      else if ( o.has_call_contract() )
         impacted_addresses.insert( o.call_contract().contract_id() )
      else if ( o.has_set_system_call() && o.set_system_call().target().has_system_call_bundle() )
         impacted_addresses.insert( o.set_system_call().target().system_call_bundle().contract_id() )
      else if ( o.has_set_system_contract() )
         impacted_addresses.insert( o.set_system_conract().contract_id() );
   }

   for ( const auto& event : trx_rec.events() )
   {
      impacted_addresses.insert( source );
      impacted_addresses.insert( std::begin( event.impacted ), std::end( event.impacted ) );
   }

   for ( const auto& address, impacted_addresses )
   {
      record_history( address, trx.id() );
   }

   // Add transaction
   history_record record;
   record.mutable_trx()->set_transaction( trx );
   record.mutable_trx()->set_receipt( trx_rec );

   state_node.put_object( space::transaction_space, trx.id(), util::converter::as< std::string >( record ) );
}

void account_history_impl::record_history( state_node_ptr state_node, const std::string& address, const std::string& id )
{
   // Get address seq num

   account_metadata meta;

   const auto result = state_node.get_object( space::account_metadata_space, address );

   if ( result )
   {
      meta = util::converter::to< account_metadata >( *result );
      meta.set_seq_num( meta.seq_num() + 1 );
   }

   // Write history
   history_index index;
   index.set_address( address );
   index.set_seq_num( meta.seq_num() );

   state_node.put_object( space::account_history, util::converter::as< std::string >( index ), id );
   state_node.put_object( space::account_metadata_space, address, util::converter::as< std::string >( meta ) );
}

} // detail

account_history::account_history( const std::vector< std::string >& whitelist ) :
   _my( std::make_unique< detail::account_history_impl >( whitelist ) )
{}

account_history::~account_history()
{
   _my->close();
}

account_history::open( const std::filesystem::path& p, fork_resolution_algorithm algo, bool reset )
{
   _my->open( p, algo, reset );
}

account_history::close()
{
   _my->close();
}

account_history::handle_block( const broadcast::block_accepted& bam )
{
   _my->handle_block( bam );
}

account_history::handle_irreversible( const broadcast::block_irreversible& irr )
{
   _my->handle_irreversible( irr );
}

} // koinos::account_history
