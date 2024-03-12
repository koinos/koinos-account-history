#include <koinos/account_history/state.hpp>

namespace koinos { namespace account_history::space {

namespace detail {

constexpr uint32_t record_id           = 1;
constexpr uint32_t account_metadata_id = 2;
constexpr uint32_t account_history_id  = 3;

const chain::object_space make_history_record()
{
  chain::object_space s;
  s.set_id( record_id );
  return s;
}

const chain::object_space make_account_metadata()
{
  chain::object_space s;
  s.set_id( account_metadata_id );
  return s;
}

const chain::object_space make_account_history()
{
  chain::object_space s;
  s.set_id( account_metadata_id );
  return s;
}

} // namespace detail

const chain::object_space history_record()
{
  static auto s = detail::make_history_record();
  return s;
}

const chain::object_space account_metadata()
{
  static auto s = detail::make_account_metadata();
  return s;
}

const chain::object_space account_history()
{
  static auto s = detail::make_account_history();
  return s;
}

}} // namespace koinos::account_history::space
