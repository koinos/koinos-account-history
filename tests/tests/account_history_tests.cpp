#include <boost/test/unit_test.hpp>

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

#include <koinos/account_history/account_history.hpp>
#include <koinos/crypto/multihash.hpp>
#include <koinos/util/conversion.hpp>
#include <koinos/util/hex.hpp>

#include <memory>

using namespace koinos;
using namespace std::string_literals;

struct account_history_fixture
{
   account_history_fixture()
   {
      initialize_logging( "koinos_account_history_test", {}, "info" );

      _state_dir = std::filesystem::temp_directory_path() / boost::filesystem::unique_path().string();
      LOG(info) << "Test temp dir: " << _state_dir.string();
      std::filesystem::create_directory( _state_dir );

      _account_history.open( _state_dir, account_history::fork_resolution_algorithm::fifo, false );
   }

   ~account_history_fixture()
   {
      boost::log::core::get()->remove_all_sinks();
      std::filesystem::remove_all( _state_dir );
   }

   account_history::account_history _account_history;
   std::filesystem::path            _state_dir;
};

BOOST_FIXTURE_TEST_SUITE( account_history_tests, account_history_fixture )

BOOST_AUTO_TEST_CASE( basic )
{
   auto alice_address   = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "alice"s ) );
   auto bob_address     = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "bob"s ) );
   auto charlie_address = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "charlie"s ) );
   auto dave_address    = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "dave"s ) );
   auto ellie_address    = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "ellie"s ) );

   broadcast::block_accepted bam1;
   bam1.mutable_block()->mutable_header()->set_previous( util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );
   bam1.mutable_block()->mutable_header()->set_height( 1 );

   auto block_1_id = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, 1 ) );
   *bam1.mutable_block()->mutable_id() = block_1_id;
   *bam1.mutable_block()->mutable_header()->mutable_signer() = alice_address;
   *bam1.mutable_receipt()->mutable_id() = block_1_id;

   auto event = bam1.mutable_receipt()->add_events();
   event->set_sequence( 0 );
   *event->mutable_source() = bob_address;
   *event->add_impacted() = alice_address;
   *event->add_impacted() = charlie_address;

   auto trx_1_id = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "trx_1"s ) );
   auto trx = bam1.mutable_block()->add_transactions();
   trx->set_id( trx_1_id );
   *trx->mutable_header()->mutable_payer() = alice_address;
   *trx->mutable_header()->mutable_payee() = ellie_address;
   auto op = trx->add_operations();
   op->mutable_call_contract()->set_contract_id( bob_address );

   event = bam1.mutable_receipt()->add_transaction_receipts()->add_events();
   event->set_source( charlie_address );
   *event->add_impacted() = alice_address;
   *event->add_impacted() = dave_address;

   // Alice should have a block event, then a trx event
   // Bob should have a block event, then a trx event
   // Charlie should have a block event, then a trx event
   // Dave should have a trx event
   // Ellie should have a trx event

   _account_history.handle_block( bam1 );

   auto records = _account_history.get_account_history( alice_address, 0, 10, true );

   BOOST_REQUIRE_EQUAL( records.size(), 2 );
   BOOST_CHECK_EQUAL( records[0].seq_num(), 0 );
   BOOST_REQUIRE( records[0].has_block() );
   BOOST_CHECK( records[0].block().receipt().id() == block_1_id );

   BOOST_CHECK_EQUAL( records[1].seq_num(), 1 );
   BOOST_REQUIRE( records[1].has_trx() );
   BOOST_CHECK( records[1].trx().transaction().id() == trx_1_id );

   records = _account_history.get_account_history( bob_address, 0, 10, true );

   BOOST_REQUIRE_EQUAL( records.size(), 2 );
   BOOST_CHECK_EQUAL( records[0].seq_num(), 0 );
   BOOST_REQUIRE( records[0].has_block() );
   BOOST_CHECK( records[0].block().receipt().id() == block_1_id );

   BOOST_CHECK_EQUAL( records[1].seq_num(), 1 );
   BOOST_REQUIRE( records[1].has_trx() );
   BOOST_CHECK( records[1].trx().transaction().id() == trx_1_id );

   records = _account_history.get_account_history( charlie_address, 0, 10, true );

   BOOST_REQUIRE_EQUAL( records.size(), 2 );
   BOOST_CHECK_EQUAL( records[0].seq_num(), 0 );
   BOOST_REQUIRE( records[0].has_block() );
   BOOST_CHECK( records[0].block().receipt().id() == block_1_id );

   BOOST_CHECK_EQUAL( records[1].seq_num(), 1 );
   BOOST_REQUIRE( records[1].has_trx() );
   BOOST_CHECK( records[1].trx().transaction().id() == trx_1_id );

   records = _account_history.get_account_history( dave_address, 0, 10, true );

   BOOST_REQUIRE_EQUAL( records.size(), 1 );
   BOOST_CHECK_EQUAL( records[0].seq_num(), 0 );
   BOOST_REQUIRE( records[0].has_trx() );
   BOOST_CHECK( records[0].trx().transaction().id() == trx_1_id );

   records = _account_history.get_account_history( ellie_address, 0, 10, true );

   BOOST_REQUIRE_EQUAL( records.size(), 1 );
   BOOST_CHECK_EQUAL( records[0].seq_num(), 0 );
   BOOST_REQUIRE( records[0].has_trx() );
   BOOST_CHECK( records[0].trx().transaction().id() == trx_1_id );
}

BOOST_AUTO_TEST_CASE( pagination )
{
   auto alice_address = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "alice"s ) );

   broadcast::block_accepted block_accept;
   block_accept.mutable_block()->set_id( util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );

   auto trx = block_accept.mutable_block()->add_transactions();
   *trx->mutable_header()->mutable_payer() = alice_address;
   block_accept.mutable_receipt()->add_transaction_receipts();

   broadcast::block_irreversible block_irr;

   BOOST_TEST_MESSAGE( "Add 1000 history records..." );

   for ( uint32_t i = 0; i < 1000; i++ )
   {
      block_accept.mutable_block()->mutable_header()->set_height( i + 1 );
      block_accept.mutable_block()->mutable_header()->set_allocated_previous( block_accept.mutable_block()->release_id() );
      block_accept.mutable_block()->set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, i ) ) );
      block_accept.mutable_block()->mutable_transactions( 0 )->set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "trx_"s + std::to_string( i ) ) ) );

      _account_history.handle_block( block_accept );
      *block_irr.mutable_topology()->mutable_id() = block_accept.block().id();

      _account_history.handle_irreversible( block_irr );
   }

   BOOST_TEST_MESSAGE( "Check history ascending" );

   for ( uint32_t i = 0; i < 1; i++ )
   {
      auto records = _account_history.get_account_history( alice_address, i * 250, 250, true );

      BOOST_REQUIRE_EQUAL( records.size(), 250 );

      for ( uint32_t j = 0; j < 250; j++ )
      {
         BOOST_CHECK_EQUAL( records[j].seq_num(), i * 250 + j );
         BOOST_REQUIRE( records[j].has_trx() );
         BOOST_CHECK( records[j].trx().transaction().id() == util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "trx_"s + std::to_string( i * 250 + j ) ) ) );
      }
   }

   BOOST_TEST_MESSAGE( "Check history descending" );

   for ( uint32_t i = 0; i < 4; i++ )
   {
      auto records = _account_history.get_account_history( alice_address, i == 0 ? 0 : 999 - i * 250, 250, false );

      BOOST_REQUIRE_EQUAL( records.size(), 250 );

      for ( uint32_t j = 0; j < 10; j++ )
      {
         BOOST_CHECK_EQUAL( records[j].seq_num(), 999 - ( i * 250 + j ) );
         BOOST_REQUIRE( records[j].has_trx() );
         BOOST_CHECK( records[j].trx().transaction().id() == util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "trx_"s + std::to_string( 999 - ( i * 250 + j ) ) ) ) );
      }
   }
}

BOOST_AUTO_TEST_SUITE_END()
