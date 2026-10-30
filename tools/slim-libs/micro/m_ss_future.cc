#include <seastar/core/future.hh>
seastar::future<int> f(){ return seastar::make_ready_future<int>(1).then([](int x){ return x+1; }); }
