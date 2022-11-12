#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/system_timer.hpp>
#include <boost/program_options.hpp>

#include <yaml-cpp/yaml.h>

#include <koinos/account_history/account_history.hpp>
#include <koinos/broadcast/broadcast.pb.h>
#include <koinos/rpc/account_history/account_history_rpc.pb.h>
#include <koinos/rpc/block_store/block_store_rpc.pb.h>
#include <koinos/rpc/chain/chain_rpc.pb.h>
#include <koinos/exception.hpp>
#include <koinos/mq/client.hpp>
#include <koinos/mq/request_handler.hpp>
#include <koinos/util/conversion.hpp>
#include <koinos/util/options.hpp>
#include <koinos/util/random.hpp>
#include <koinos/util/services.hpp>

#include "git_version.h"

#define FIFO_ALGORITHM                 "fifo"
#define BLOCK_TIME_ALGORITHM           "block-time"
#define POB_ALGORITHM                  "pob"

#define HELP_OPTION                    "help"
#define VERSION_OPTION                 "version"
#define BASEDIR_OPTION                 "basedir"
#define AMQP_OPTION                    "amqp"
#define AMQP_DEFAULT                   "amqp://guest:guest@localhost:5672/"
#define LOG_LEVEL_OPTION               "log-level"
#define LOG_LEVEL_DEFAULT              "info"
#define INSTANCE_ID_OPTION             "instance-id"
#define STATEDIR_OPTION                "statedir"
#define STATEDIR_DEFAULT               "db"
#define RESET_OPTION                   "reset"
#define JOBS_OPTION                    "jobs"
#define JOBS_DEFAULT                   uint64_t( 2 )
#define FORK_ALGORITHM_OPTION          "fork-algorithm"
#define FORK_ALGORITHM_DEFAULT         FIFO_ALGORITHM
#define WHITELIST_OPTION               "whitelist"

KOINOS_DECLARE_EXCEPTION( service_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( invalid_argument, service_exception );

using namespace boost;
using namespace koinos;

using log_func_type = std::function< void( const boost::system::error_code&, std::shared_ptr< account_history::account_history > ) >;
using sync_func_type = std::function< void( const boost::system::error_code&, std::shared_ptr< account_history::account_history > ) >;

const std::string& version_string();

bool sync_func_impl( const boost::system::error_code& ec, koinos::mq::client& client, std::shared_ptr< account_history::account_history > ah )
{
   // Get chain head info, if chain LIB is greater than AH LIB, get blocks between chain head and
   // AH LIB to catch up with chain.
   rpc::chain::chain_request chain_req;
   chain_req.mutable_get_head_info();

   auto future = client.rpc( util::service::chain, util::converter::as< std::string >( chain_req ), 1000ms, mq::retry_policy::exponential_backoff );
   rpc::chain::chain_response chain_resp;
   chain_resp.ParseFromString( future.get() );

   if( !chain_resp.has_error() && chain_resp.has_get_head_info() )
   {
      auto chain_lib = chain_resp.get_head_info().last_irreversible_block();
      auto ah_lib = ah->get_lib_height();

      if ( ah_lib < chain_lib )
      {
         uint64_t request_size = std::min( uint64_t( 500 ), chain_resp.get_head_info().head_topology().height() - ( ah_lib + 1 ) );

         rpc::block_store::block_store_request bs_req;
         bs_req.mutable_get_blocks_by_height()->set_allocated_head_block_id( chain_resp.mutable_get_head_info()->mutable_head_topology()->release_id() );
         bs_req.mutable_get_blocks_by_height()->set_ancestor_start_height( ah_lib + 1 );
         bs_req.mutable_get_blocks_by_height()->set_num_blocks( request_size );
         bs_req.mutable_get_blocks_by_height()->set_return_block( true );
         bs_req.mutable_get_blocks_by_height()->set_return_receipt( true );

         auto future = client.rpc( util::service::block_store, util::converter::as< std::string >( bs_req ), 1000ms, mq::retry_policy::exponential_backoff );
         rpc::block_store::block_store_response bs_resp;
         bs_resp.ParseFromString( future.get() );

         if ( !bs_resp.has_error() && bs_resp.has_get_blocks_by_height() )
         {
            auto blocks_by_height = bs_resp.mutable_get_blocks_by_height();

            for ( uint32_t i = 0; i < blocks_by_height->block_items_size(); i++ )
            {
               broadcast::block_accepted block_accept;
               block_accept.set_allocated_block( blocks_by_height->mutable_block_items( i )->release_block() );
               block_accept.set_allocated_receipt( blocks_by_height->mutable_block_items( i )->release_receipt() );

               ah->handle_block( block_accept );

               if ( block_accept.block().header().height() <= chain_lib )
               {
                  broadcast::block_irreversible block_irr;
                  block_irr.mutable_topology()->set_allocated_id( block_accept.mutable_block()->release_id() );
                  block_irr.mutable_topology()->set_height( block_accept.block().header().height() );
                  block_irr.mutable_topology()->set_allocated_previous( block_accept.mutable_block()->mutable_header()->release_previous() );

                  ah->handle_irreversible( block_irr );
               }
            }

            if ( request_size == 500 )
            {
               LOG(info) << "Synced to block " << ah_lib + request_size;
               return true;
            }
         }
      }
   }

   return false;
};

int main( int argc, char** argv )
{
   std::atomic< bool > stopped = false;
   int retcode = EXIT_SUCCESS;
   std::vector< std::thread > threads;

   boost::asio::io_context main_ioc, server_ioc, client_ioc;
   auto request_handler = koinos::mq::request_handler( server_ioc );
   auto client = koinos::mq::client( client_ioc );
   auto log_timer = boost::asio::system_timer( server_ioc );
   auto sync_timer = boost::asio::system_timer( server_ioc );

   log_func_type log_func = [&]( const boost::system::error_code& ec, std::shared_ptr< account_history::account_history > ah )
   {
      if ( ec == boost::asio::error::operation_aborted )
         return;

      LOG(info) << "Recently added " << ah->get_recent_entries_count() << " account history entries";

      log_timer.expires_after( 60s );
      log_timer.async_wait( boost::bind( log_func, boost::asio::placeholders::error, ah ) );
   };

   sync_func_type sync_func = [&]( const boost::system::error_code& ec, std::shared_ptr< account_history::account_history > ah )
   {
      if ( ec == boost::asio::error::operation_aborted )
         return;

      if ( !sync_func_impl( ec, client, ah ) )
      {
         sync_timer.expires_after( 60s );
      }

      sync_timer.async_wait( boost::bind( sync_func, boost::asio::placeholders::error, ah ) );
   };

   try
   {
      program_options::options_description options;
      options.add_options()
         (HELP_OPTION                  ",h", "Print this help message and exit")
         (VERSION_OPTION               ",v", "Print version string and exit")
         (BASEDIR_OPTION               ",d", program_options::value< std::string >()->default_value( util::get_default_base_directory().string() ),
            "Koinos base directory")
         (AMQP_OPTION                  ",a", program_options::value< std::string >(), "AMQP server URL")
         (LOG_LEVEL_OPTION             ",l", program_options::value< std::string >(), "The log filtering level")
         (INSTANCE_ID_OPTION           ",i", program_options::value< std::string >(), "An ID that uniquely identifies the instance")
         (JOBS_OPTION                  ",j", program_options::value< uint64_t >(), "The number of worker jobs")
         (STATEDIR_OPTION                  , program_options::value< std::string >(),
            "The location of the blockchain state files (absolute path or relative to basedir/chain)")
         (RESET_OPTION                     , program_options::value< bool >(), "Reset the database")
         (FORK_ALGORITHM_OPTION        ",f", program_options::value< std::string >(), "The fork resolution algorithm to use. Can be 'fifo', 'pob', or 'block-time'. (Default: 'fifo')");

      program_options::variables_map args;
      program_options::store( program_options::parse_command_line( argc, argv, options ), args );

      if ( args.count( HELP_OPTION ) )
      {
         std::cout << options << std::endl;
         return EXIT_SUCCESS;
      }

      if ( args.count( VERSION_OPTION ) )
      {
         const auto& v_str = version_string();
         std::cout.write( v_str.c_str(), v_str.size() );
         std::cout << std::endl;
         return EXIT_SUCCESS;
      }

      auto basedir = std::filesystem::path{ args[ BASEDIR_OPTION ].as< std::string >() };
      if ( basedir.is_relative() )
         basedir = std::filesystem::current_path() / basedir;

      YAML::Node config;
      YAML::Node global_config;
      YAML::Node account_history_config;

      auto yaml_config = basedir / "config.yml";
      if ( !std::filesystem::exists( yaml_config ) )
      {
         yaml_config = basedir / "config.yaml";
      }

      if ( std::filesystem::exists( yaml_config ) )
      {
         config = YAML::LoadFile( yaml_config );
         global_config = config[ "global" ];
         account_history_config = config[ "account_history" ];
      }

      auto amqp_url              = util::get_option< std::string >( AMQP_OPTION, AMQP_DEFAULT, args, account_history_config, global_config );
      auto log_level             = util::get_option< std::string >( LOG_LEVEL_OPTION, LOG_LEVEL_DEFAULT, args, account_history_config, global_config );
      auto instance_id           = util::get_option< std::string >( INSTANCE_ID_OPTION, util::random_alphanumeric( 5 ), args, account_history_config, global_config );
      auto jobs                  = util::get_option< uint64_t >( JOBS_OPTION, std::max( JOBS_DEFAULT, uint64_t( std::thread::hardware_concurrency() ) ), args, account_history_config, global_config );
      auto statedir              = std::filesystem::path( util::get_option< std::string >( STATEDIR_OPTION, STATEDIR_DEFAULT, args, account_history_config, global_config ) );
      auto reset                 = util::get_option< bool >( RESET_OPTION, false, args, account_history_config, global_config );
      auto fork_algorithm_option = util::get_option< std::string >( FORK_ALGORITHM_OPTION, FORK_ALGORITHM_DEFAULT, args, account_history_config, global_config );
      auto whitelist_addresses   = util::get_options< std::string >( WHITELIST_OPTION, args, account_history_config, global_config );

      koinos::initialize_logging( "account_history", instance_id, log_level, basedir / "account_history" / "logs" );

      LOG(info) << version_string();

      KOINOS_ASSERT( jobs > 1, invalid_argument, "jobs must be greater than 1" );

      if ( config.IsNull() )
      {
         LOG(warning) << "Could not find config (config.yml or config.yaml expected), using default values";
      }

      account_history::fork_resolution_algorithm fork_algorithm;

      if ( fork_algorithm_option == FIFO_ALGORITHM )
      {
         LOG(info) << "Using fork resolution algorithm: " << FIFO_ALGORITHM;
         fork_algorithm = account_history::fork_resolution_algorithm::fifo;
      }
      else if ( fork_algorithm_option == BLOCK_TIME_ALGORITHM )
      {
         LOG(info) << "Using fork resolution algorithm: " << BLOCK_TIME_ALGORITHM;
         fork_algorithm = account_history::fork_resolution_algorithm::block_time;
      }
      else if ( fork_algorithm_option == POB_ALGORITHM )
      {
         LOG(info) << "Using fork resolution algorithm: " << POB_ALGORITHM;
         fork_algorithm = account_history::fork_resolution_algorithm::pob;
      }
      else
      {
         KOINOS_THROW( invalid_argument, "${a} is not a valid fork algorithm", ("a", fork_algorithm_option) );
      }

      std::vector< std::string > whitelist;

      for ( const auto& address : whitelist_addresses )
      {
         try
         {
            whitelist.emplace_back( util::from_hex< std::string >( address ) );
         }
         catch( const std::exception& e )
         {
            KOINOS_THROW( invalid_argument, "could not parse whitelist address '${a}'", ("a", address) );
         }
      }

      LOG(info) << "Starting account history...";
      LOG(info) << "Number of jobs: " << jobs;

      boost::asio::signal_set signals( server_ioc );
      signals.add( SIGINT );
      signals.add( SIGTERM );
#if defined( SIGQUIT )
      signals.add( SIGQUIT );
#endif

      signals.async_wait( [&]( const boost::system::error_code& err, int num )
      {
         LOG(info) << "Caught signal, shutting down...";
         stopped = true;
         main_ioc.stop();
      } );

      threads.emplace_back( [&]() { client_ioc.run(); } );
      threads.emplace_back( [&]() { client_ioc.run(); } );

      for ( std::size_t i = 0; i < jobs; i++ )
         threads.emplace_back( [&]() { server_ioc.run(); } );

      std::shared_ptr< koinos::account_history::account_history > account_history = std::make_shared< koinos::account_history::account_history >( whitelist );

      request_handler.add_rpc_handler(
         "account_history",
         [&]( const std::string& msg ) -> std::string
         {
            koinos::rpc::account_history::account_history_request args;
            koinos::rpc::account_history::account_history_response resp;

            if ( args.ParseFromString( msg ) )
            {
               try
               {
                  switch( args.request_case() )
                  {
                     case rpc::account_history::account_history_request::RequestCase::kGetAccountHistory:
                     {
                        const auto& a = args.get_account_history();
                        const auto& values = account_history->get_account_history(
                           a.address(),
                           a.seq_num(),
                           a.limit(),
                           a.ascending(),
                           a.from_lib() );

                        for ( const auto& v : values )
                        {
                           resp.mutable_get_account_history()->add_values()->CopyFrom( v );
                        }

                        break;
                     }
                     case rpc::account_history::account_history_request::RequestCase::kReserved:
                        resp.mutable_reserved();
                        break;
                     default:
                        resp.mutable_error()->set_message( "Error: attempted to call unknown rpc" );
                  }
               }
               catch( const koinos::exception& e )
               {
                  auto error = resp.mutable_error();
                  error->set_message( e.what() );
                  error->set_data( e.get_stacktrace() );
               }
               catch( std::exception& e )
               {
                  resp.mutable_error()->set_message( e.what() );
               }
               catch( ... )
               {
                  LOG(error) << "Unexpected error while handling rpc: " << args.ShortDebugString();
                  resp.mutable_error()->set_message( "Unexpected error while handling rpc" );
               }
            }
            else
            {
               LOG(warning) << "Received bad message";
               resp.mutable_error()->set_message( "Received bad message" );
            }

            std::stringstream out;
            resp.SerializeToOstream( &out );
            return out.str();
         }
      );

      request_handler.add_broadcast_handler(
         "koinos.block.accept",
         [&]( const std::string& msg )
         {
            koinos::broadcast::block_accepted block_accept;

            if ( !block_accept.ParseFromString( msg ) )
            {
               LOG(warning) << "Could not parse block accepted broadcast";
               return;
            }

            try
            {
               account_history->handle_block( block_accept );
            }
            catch ( const std::exception& e )
            {
               LOG(info) << "Could not process block accepted: " << e.what();
            }
         }
      );

      request_handler.add_broadcast_handler(
         "koinos.block.irreversible",
         [&]( const std::string& msg )
         {
            koinos::broadcast::block_irreversible block_irr;

            if ( !block_irr.ParseFromString( msg ) )
            {
               LOG(warning) << "Could not parse transaction failure broadcast";
               return;
            }

            try
            {
               account_history->handle_irreversible( block_irr );
            }
            catch ( const std::exception& e )
            {
               LOG(info) << "Could not process block irreversibility: " << e.what();
            }
         }
      );

      log_timer.expires_after( 60s );
      log_timer.async_wait( boost::bind( log_func, boost::asio::placeholders::error, account_history ) );

      if ( statedir.is_relative() )
         statedir = basedir / "account_history" / statedir;

      if ( !std::filesystem::exists( statedir ) )
         std::filesystem::create_directories( statedir );

      account_history->open( statedir, fork_algorithm, reset );

      LOG(info) << "Connecting AMQP client...";
      client.connect( amqp_url );
      LOG(info) << "Established AMQP client connection to the server";

      LOG(info) << "Attempting to connect to block_store...";
      rpc::block_store::block_store_request b_req;
      b_req.mutable_reserved();
      client.rpc( util::service::block_store, b_req.SerializeAsString() ).get();
      LOG(info) << "Established connection to block_store";

      LOG(info) << "Attempting to connect to chain...";
      rpc::chain::chain_request c_req;
      c_req.mutable_reserved();
      client.rpc( util::service::chain, c_req.SerializeAsString() ).get();
      LOG(info) << "Established connection to chain";

      LOG(info) << "Connecting AMQP request handler...";
      request_handler.connect( amqp_url );
      LOG(info) << "Established request handler connection to the AMQP server";

      sync_timer.async_wait( boost::bind( sync_func, boost::asio::placeholders::error, account_history ) );

      LOG(info) << "Listening for requests over AMQP";
      auto work = asio::make_work_guard( main_ioc );
      main_ioc.run();
   }
   catch ( const invalid_argument& e )
   {
      LOG(error) << "Invalid argument: " << e.what();
      retcode = EXIT_FAILURE;
   }
   catch ( const koinos::exception& e )
   {
      if ( !stopped )
      {
         LOG(fatal) << "An unexpected error has occurred: " << e.what();
         retcode = EXIT_FAILURE;
      }
   }
   catch ( const boost::exception& e )
   {
      LOG(fatal) << "An unexpected error has occurred: " << boost::diagnostic_information( e );
      retcode = EXIT_FAILURE;
   }
   catch ( const std::exception& e )
   {
      LOG(fatal) << "An unexpected error has occurred: " << e.what();
      retcode = EXIT_FAILURE;
   }
   catch ( ... )
   {
      LOG(fatal) << "An unexpected error has occurred";
      retcode = EXIT_FAILURE;
   }

   log_timer.cancel();
   sync_timer.cancel();

   for ( auto& t : threads )
      t.join();

   LOG(info) << "Shut down gracefully";

   return retcode;
}

const std::string& version_string()
{
   static std::string v_str = "Koinos Account History v";
   v_str += std::to_string( KOINOS_MAJOR_VERSION ) + "." + std::to_string( KOINOS_MINOR_VERSION ) + "." + std::to_string( KOINOS_PATCH_VERSION );
   v_str += " (" + std::string( KOINOS_GIT_HASH ) + ")";
   return v_str;
}
