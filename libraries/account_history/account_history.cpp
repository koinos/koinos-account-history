#include <koinos/account_history/account_history.hpp>
#include <koinos/account_history/state.hpp>

#include <koinos/state_db/state_db.hpp>
#include <koinos/util/conversion.hpp>

#include <koinos/account_history/account_history.pb.h>

namespace koinos::account_history {

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

      void add_transaction( state_db::state_node_ptr state_node, const protocol::transaction&, const protocol::transaction_receipt& );
      void record_history( state_db::state_node_ptr state_node, const std::string& address, const std::string& id );

      std::vector< account_history_entry > get_account_history( const std::string& account, uint32_t seq_num, uint32_t limit, bool ascending ) const;

   private:
      state_db::database _db;
      const std::vector< std::string >& _whitelist;
};

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

void account_history_impl::handle_irreversible( const broadcast::block_irreversible& block_irr )
{
   auto block_id = util::converter::to< crypto::multihash >( block_irr.topology().id() );
   auto db_lock = _db.get_unique_lock();
   auto lib = _db.get_node( block_id, db_lock );

   if ( lib )
   {
      _db.commit_node( block_id, db_lock );
   }
}

void account_history_impl::handle_block( const broadcast::block_accepted& block_accept )
{
   auto block_id = util::converter::to< crypto::multihash >( block_accept.block().id() );
   auto previous_id = util::converter::to< crypto::multihash >( block_accept.block().header().previous() );
   auto db_lock = _db.get_shared_lock();
   auto state_node = _db.create_writable_node( previous_id, block_id, block_accept.block().header(), db_lock );

   if ( !state_node )
      return;

   try
   {
      std::set< std::string > impacted_addresses;

      // Add signer address to impacted for block
      impacted_addresses.insert( block_accept.block().header().signer() );

      // Add event source and impacted addresses to impacted for block
      for ( const auto& event : block_accept.receipt().events() )
      {
         impacted_addresses.insert( event.source() );
         impacted_addresses.insert( std::begin( event.impacted() ), std::end( event.impacted() ) );
      }

      // Add block id as record for every impacted address
      for ( const auto& address : impacted_addresses )
      {
         record_history( state_node, address, block_accept.block().id() );
      }

      // Add block record
      history_record record;
      *record.mutable_block()->mutable_header() = block_accept.block().header();
      *record.mutable_block()->mutable_receipt() = block_accept.receipt();

      auto record_str = util::converter::as< std::string >( record );
      state_node->put_object( space::history_record(), block_accept.block().id(), &record_str );

      // Add records for all contained transactions
      for ( std::size_t i = 0; i < block_accept.block().transactions().size(); i++ )
      {
         add_transaction( state_node, block_accept.block().transactions( 0 ), block_accept.receipt().transaction_receipts( 0 ) );
      }
   }
   catch ( ... )
   {
      _db.discard_node( block_id, db_lock );
      throw;
   }

   _db.finalize_node( block_id, db_lock );
}

void account_history_impl::add_transaction( state_db::state_node_ptr state_node, const protocol::transaction& trx, const protocol::transaction_receipt& trx_rec )
{
   std::set< std::string > impacted_addresses;

   // Add payer to impacted
   impacted_addresses.insert( trx.header().payer() );

   // Add payee to impacted, if it exists
   if ( trx.header().payee().size() )
      impacted_addresses.insert( trx.header().payee() );

   // For every op, add relevant addresses to impacted
   for ( const auto& op : trx.operations() )
   {
      if ( op.has_upload_contract() )
      {
         impacted_addresses.insert( op.upload_contract().contract_id() );
      }
      else if ( op.has_call_contract() )
      {
         impacted_addresses.insert( op.call_contract().contract_id() );
      }
      else if ( op.has_set_system_call() && op.set_system_call().target().has_system_call_bundle() )
      {
         impacted_addresses.insert( op.set_system_call().target().system_call_bundle().contract_id() );
      }
      else if ( op.has_set_system_contract() )
      {
         impacted_addresses.insert( op.set_system_contract().contract_id() );
      }
   }

   // For every event, add source and impacted to impacted
   for ( const auto& event : trx_rec.events() )
   {
      impacted_addresses.insert( event.source() );
      impacted_addresses.insert( std::begin( event.impacted() ), std::end( event.impacted() ) );
   }

   // Add trx id as record for every impacted address
   for ( const auto& address : impacted_addresses )
   {
      record_history( state_node, address, trx.id() );
   }

   // Add transaction record
   history_record record;
   *record.mutable_trx()->mutable_transaction() = trx ;
   *record.mutable_trx()->mutable_receipt() = trx_rec;

   auto record_str = util::converter::as< std::string >( record );
   state_node->put_object( space::history_record(), trx.id(), &record_str );
}

void account_history_impl::record_history( state_db::state_node_ptr state_node, const std::string& address, const std::string& id )
{
   // Get address seq num
   account_metadata meta;

   const auto result = state_node->get_object( space::account_metadata(), address );

   // Increment seq_num if record already exists
   if ( result )
   {
      meta = util::converter::to< account_metadata >( *result );
      meta.set_seq_num( meta.seq_num() + 1 );
   }

   // Write history
   history_index index;
   index.set_address( address );
   index.set_seq_num( meta.seq_num() );

   state_node->put_object( space::account_history(), util::converter::as< std::string >( index ), &id );

   auto meta_str = util::converter::as< std::string >( meta );
   state_node->put_object( space::account_metadata(), address, &meta_str );
}

std::vector< account_history_entry > account_history_impl::get_account_history( const std::string& address, uint32_t seq_num, uint32_t limit, bool ascending ) const
{
   // assert (limit <= 1000)

   history_index index;
   index.set_address( address );
   index.set_seq_num( seq_num );

   auto state_node = _db.get_head( _db.get_shared_lock() );

   if ( !ascending && seq_num == 0 )
   {
      const auto result = state_node->get_object( space::account_metadata(), address );

      if ( result )
      {
         index.set_seq_num( util::converter::to< account_metadata >( *result ).seq_num() );
      }
   }

   std::vector< account_history_entry > entries;
   entries.reserve( limit );

   while ( entries.size() < limit )
   {
      std::string id;

      auto id_ptr = state_node->get_object( space::account_history(), util::converter::as< std::string >( index ) );
      if ( !id_ptr )
         break;

      auto record_ptr = state_node->get_object( space::history_record(), *id_ptr );
      // assert record_ptr
      auto record = util::converter::to< history_record >( *record_ptr );

      account_history_entry entry;
      entry.set_seq_num( index.seq_num() );
      if ( record.has_block() )
      {
         entry.set_allocated_block( record.release_block() );
      }
      else
      {
         entry.set_allocated_trx( record.release_trx() );
      }

      entries.emplace_back( std::move( entry ) );

      if ( ascending )
      {
         index.set_seq_num( index.seq_num() + 1 );
      }
      else
      {
         index.set_seq_num( index.seq_num() - 1 );
      }
   }

   return entries;
}

} // detail

account_history::account_history( const std::vector< std::string >& whitelist ) :
   _my( std::make_unique< detail::account_history_impl >( whitelist ) )
{}

account_history::~account_history()
{
   _my->close();
}

void account_history::open( const std::filesystem::path& p, fork_resolution_algorithm algo, bool reset )
{
   _my->open( p, algo, reset );
}

void account_history::close()
{
   _my->close();
}

void account_history::handle_block( const broadcast::block_accepted& block_accept )
{
   _my->handle_block( block_accept );
}

void account_history::handle_irreversible( const broadcast::block_irreversible& irr )
{
   _my->handle_irreversible( irr );
}

std::vector< account_history_entry > account_history::get_account_history( const std::string& address, uint32_t seq_num, uint32_t limit, bool ascending ) const
{
   return _my->get_account_history( address, seq_num, limit, ascending );
}

} // koinos::account_history
