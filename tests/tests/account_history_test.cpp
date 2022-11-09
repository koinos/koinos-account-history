#include <boost/test/unit_test.hpp>

#include <koinos/account_history/account_history.hpp>

#include <memory>

using namespace koinos;
using namespace std::chrono_literals;

struct account_history_fixture
{
   account_history_fixture()
   {

   }
};

BOOST_FIXTURE_TEST_SUITE( account_history_tests, account_history_fixture )

BOOST_AUTO_TEST_CASE( account_history_basic_test )
{

}

BOOST_AUTO_TEST_SUITE_END()
