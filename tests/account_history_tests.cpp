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
    LOG( info ) << "Test temp dir: " << _state_dir.string();
    std::filesystem::create_directory( _state_dir );

    _account_history.open( _state_dir, state_db::fork_resolution_algorithm::fifo, false );
  }

  ~account_history_fixture()
  {
    boost::log::core::get()->remove_all_sinks();
    std::filesystem::remove_all( _state_dir );
  }

  account_history::account_history _account_history;
  std::filesystem::path _state_dir;
};

BOOST_FIXTURE_TEST_SUITE( account_history_tests, account_history_fixture )

BOOST_AUTO_TEST_CASE( basic )
{
  auto alice_address   = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "alice"s ) );
  auto bob_address     = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "bob"s ) );
  auto charlie_address = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "charlie"s ) );
  auto dave_address    = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "dave"s ) );
  auto eve_address     = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "eve"s ) );

  broadcast::block_accepted block_accept;
  block_accept.mutable_block()->mutable_header()->set_previous(
    util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );
  block_accept.mutable_block()->mutable_header()->set_height( 1 );

  auto block_1_id = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, 1 ) );
  *block_accept.mutable_block()->mutable_id()                       = block_1_id;
  *block_accept.mutable_block()->mutable_header()->mutable_signer() = alice_address;
  *block_accept.mutable_receipt()->mutable_id()                     = block_1_id;

  auto event = block_accept.mutable_receipt()->add_events();
  event->set_sequence( 0 );
  *event->mutable_source() = bob_address;
  *event->add_impacted()   = alice_address;
  *event->add_impacted()   = charlie_address;

  auto trx_1_id = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "trx_1"s ) );
  auto trx      = block_accept.mutable_block()->add_transactions();
  trx->set_id( trx_1_id );
  *trx->mutable_header()->mutable_payer() = alice_address;
  *trx->mutable_header()->mutable_payee() = eve_address;
  auto op                                 = trx->add_operations();
  op->mutable_call_contract()->set_contract_id( bob_address );

  event = block_accept.mutable_receipt()->add_transaction_receipts()->add_events();
  event->set_source( charlie_address );
  *event->add_impacted() = alice_address;
  *event->add_impacted() = dave_address;

  // Alice should have a block event, then a trx event
  // Bob should have a block event, then a trx event
  // Charlie should have a block event, then a trx event
  // Dave should have a trx event
  // Eve should have a trx event

  BOOST_TEST_MESSAGE( "Adding block to test adding history" );

  _account_history.handle_block( block_accept );

  BOOST_TEST_MESSAGE( "Checking account history" );

  rpc::account_history::get_account_history_request req;
  *req.mutable_address() = alice_address;
  req.set_seq_num( 0 );
  req.set_limit( 10 );
  req.set_ascending( true );
  req.set_irreversible( false );

  auto resp = _account_history.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 2 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_block() );
  BOOST_CHECK( resp.values( 0 ).block().receipt().id() == block_1_id );

  BOOST_CHECK_EQUAL( resp.values( 1 ).seq_num(), 1 );
  BOOST_REQUIRE( resp.values( 1 ).has_trx() );
  BOOST_CHECK( resp.values( 1 ).trx().transaction().id() == trx_1_id );

  *req.mutable_address() = bob_address;
  resp                   = _account_history.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 2 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_block() );
  BOOST_CHECK( resp.values( 0 ).block().receipt().id() == block_1_id );

  BOOST_CHECK_EQUAL( resp.values( 1 ).seq_num(), 1 );
  BOOST_REQUIRE( resp.values( 1 ).has_trx() );
  BOOST_CHECK( resp.values( 1 ).trx().transaction().id() == trx_1_id );

  *req.mutable_address() = charlie_address;
  resp                   = _account_history.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 2 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_block() );
  BOOST_CHECK( resp.values( 0 ).block().receipt().id() == block_1_id );

  BOOST_CHECK_EQUAL( resp.values( 1 ).seq_num(), 1 );
  BOOST_REQUIRE( resp.values( 1 ).has_trx() );
  BOOST_CHECK( resp.values( 1 ).trx().transaction().id() == trx_1_id );

  *req.mutable_address() = dave_address;
  resp                   = _account_history.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 1 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_trx() );
  BOOST_CHECK( resp.values( 0 ).trx().transaction().id() == trx_1_id );

  *req.mutable_address() = eve_address;
  resp                   = _account_history.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 1 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_trx() );
  BOOST_CHECK( resp.values( 0 ).trx().transaction().id() == trx_1_id );

  // If we read from LIB, no records should be returned.
  BOOST_TEST_MESSAGE( "Checking history from LIB" );

  *req.mutable_address() = alice_address;
  req.set_irreversible( true );
  resp = _account_history.get_account_history( req );
  BOOST_CHECK_EQUAL( resp.values_size(), 0 );

  *req.mutable_address() = bob_address;
  resp                   = _account_history.get_account_history( req );
  BOOST_CHECK_EQUAL( resp.values_size(), 0 );

  *req.mutable_address() = charlie_address;
  resp                   = _account_history.get_account_history( req );
  BOOST_CHECK_EQUAL( resp.values_size(), 0 );

  *req.mutable_address() = dave_address;
  resp                   = _account_history.get_account_history( req );
  BOOST_CHECK_EQUAL( resp.values_size(), 0 );

  *req.mutable_address() = eve_address;
  resp                   = _account_history.get_account_history( req );
  BOOST_CHECK_EQUAL( resp.values_size(), 0 );

  BOOST_TEST_MESSAGE( "Incrementing LIB and checking history advances when reading from LIB" );

  broadcast::block_irreversible block_irr;
  *block_irr.mutable_topology()->mutable_id() = block_accept.block().id();

  _account_history.handle_irreversible( block_irr );

  *req.mutable_address() = alice_address;
  resp                   = _account_history.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 2 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_block() );
  BOOST_CHECK( resp.values( 0 ).block().receipt().id() == block_1_id );

  BOOST_CHECK_EQUAL( resp.values( 1 ).seq_num(), 1 );
  BOOST_REQUIRE( resp.values( 1 ).has_trx() );
  BOOST_CHECK( resp.values( 1 ).trx().transaction().id() == trx_1_id );

  *req.mutable_address() = bob_address;
  resp                   = _account_history.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 2 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_block() );
  BOOST_CHECK( resp.values( 0 ).block().receipt().id() == block_1_id );

  BOOST_CHECK_EQUAL( resp.values( 1 ).seq_num(), 1 );
  BOOST_REQUIRE( resp.values( 1 ).has_trx() );
  BOOST_CHECK( resp.values( 1 ).trx().transaction().id() == trx_1_id );

  *req.mutable_address() = charlie_address;
  resp                   = _account_history.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 2 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_block() );
  BOOST_CHECK( resp.values( 0 ).block().receipt().id() == block_1_id );

  BOOST_CHECK_EQUAL( resp.values( 1 ).seq_num(), 1 );
  BOOST_REQUIRE( resp.values( 1 ).has_trx() );
  BOOST_CHECK( resp.values( 1 ).trx().transaction().id() == trx_1_id );

  *req.mutable_address() = dave_address;
  resp                   = _account_history.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 1 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_trx() );
  BOOST_CHECK( resp.values( 0 ).trx().transaction().id() == trx_1_id );

  *req.mutable_address() = eve_address;
  resp                   = _account_history.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 1 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_trx() );
  BOOST_CHECK( resp.values( 0 ).trx().transaction().id() == trx_1_id );
}

BOOST_AUTO_TEST_CASE( pagination )
{
  auto alice_address = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "alice"s ) );

  broadcast::block_accepted block_accept;
  block_accept.mutable_block()->set_id(
    util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );

  auto trx                                = block_accept.mutable_block()->add_transactions();
  *trx->mutable_header()->mutable_payer() = alice_address;
  block_accept.mutable_receipt()->add_transaction_receipts();

  broadcast::block_irreversible block_irr;

  BOOST_TEST_MESSAGE( "Add 1000 history resp..." );

  for( uint32_t i = 0; i < 1'000; i++ )
  {
    block_accept.mutable_block()->mutable_header()->set_height( i + 1 );
    block_accept.mutable_block()->mutable_header()->set_allocated_previous(
      block_accept.mutable_block()->release_id() );
    block_accept.mutable_block()->set_id(
      util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, i ) ) );
    block_accept.mutable_block()->mutable_transactions( 0 )->set_id( util::converter::as< std::string >(
      crypto::hash( crypto::multicodec::sha2_256, "trx_"s + std::to_string( i ) ) ) );

    _account_history.handle_block( block_accept );
    *block_irr.mutable_topology()->mutable_id() = block_accept.block().id();

    _account_history.handle_irreversible( block_irr );
  }

  BOOST_TEST_MESSAGE( "Check history ascending" );

  rpc::account_history::get_account_history_request req;
  *req.mutable_address() = alice_address;
  req.set_seq_num( 0 );
  req.set_limit( 250 );
  req.set_ascending( true );
  req.set_irreversible( false );

  for( uint32_t i = 0; i < 1; i++ )
  {
    req.set_seq_num( i * 250 );
    auto resp = _account_history.get_account_history( req );

    BOOST_REQUIRE_EQUAL( resp.values_size(), 250 );

    for( uint32_t j = 0; j < 250; j++ )
    {
      BOOST_CHECK_EQUAL( resp.values( j ).seq_num(), i * 250 + j );
      BOOST_REQUIRE( resp.values( j ).has_trx() );
      BOOST_CHECK( resp.values( j ).trx().transaction().id()
                   == util::converter::as< std::string >(
                     crypto::hash( crypto::multicodec::sha2_256, "trx_"s + std::to_string( i * 250 + j ) ) ) );
    }
  }

  BOOST_TEST_MESSAGE( "Check history descending" );

  req.set_ascending( false );
  req.clear_seq_num();

  for( uint32_t i = 0; i < 4; i++ )
  {
    if( i != 0 )
    {
      req.set_seq_num( 999 - i * 250 );
    }

    auto resp = _account_history.get_account_history( req );

    BOOST_REQUIRE_EQUAL( resp.values_size(), 250 );

    for( uint32_t j = 0; j < 10; j++ )
    {
      BOOST_CHECK_EQUAL( resp.values( j ).seq_num(), 999 - ( i * 250 + j ) );
      BOOST_REQUIRE( resp.values( j ).has_trx() );
      BOOST_CHECK(
        resp.values( j ).trx().transaction().id()
        == util::converter::as< std::string >(
          crypto::hash( crypto::multicodec::sha2_256, "trx_"s + std::to_string( 999 - ( i * 250 + j ) ) ) ) );
    }
  }
}

BOOST_AUTO_TEST_CASE( whitelist )
{
  auto alice_address = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "alice"s ) );
  auto bob_address   = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "bob"s ) );

  broadcast::block_accepted block_accept;
  block_accept.mutable_block()->mutable_header()->set_previous(
    util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );
  block_accept.mutable_block()->mutable_header()->set_height( 1 );

  auto block_1_id = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, 1 ) );
  *block_accept.mutable_block()->mutable_id()                       = block_1_id;
  *block_accept.mutable_block()->mutable_header()->mutable_signer() = alice_address;
  *block_accept.mutable_receipt()->mutable_id()                     = block_1_id;

  auto event = block_accept.mutable_receipt()->add_events();
  event->set_sequence( 0 );
  *event->mutable_source() = bob_address;

  std::set< std::string > whitelist{ alice_address };

  _account_history.close();

  account_history::account_history ah( whitelist );
  ah.open( _state_dir, state_db::fork_resolution_algorithm::fifo, false );

  BOOST_TEST_MESSAGE( "Handling block with whitelist enabled" );

  ah.handle_block( block_accept );

  BOOST_TEST_MESSAGE( "Checking history is only the whitelisted address" );

  rpc::account_history::get_account_history_request req;
  *req.mutable_address() = alice_address;
  req.set_seq_num( 0 );
  req.set_limit( 10 );
  req.set_ascending( true );
  req.set_irreversible( false );

  auto resp = ah.get_account_history( req );

  BOOST_REQUIRE_EQUAL( resp.values_size(), 1 );
  BOOST_CHECK_EQUAL( resp.values( 0 ).seq_num(), 0 );
  BOOST_REQUIRE( resp.values( 0 ).has_block() );
  BOOST_CHECK( resp.values( 0 ).block().receipt().id() == block_1_id );

  *req.mutable_address() = bob_address;
  resp                   = ah.get_account_history( req );
  BOOST_CHECK_EQUAL( resp.values_size(), 0 );
}

BOOST_AUTO_TEST_SUITE_END()
