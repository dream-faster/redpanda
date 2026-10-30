#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/std.h>
#include <fmt/ranges.h>
std::string f(int x){ return fmt::format("{} {}", x, "a"); }
