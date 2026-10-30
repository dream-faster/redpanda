#include <expected>
#include <system_error>
std::expected<int,std::error_code> f(){ return 1; }
