#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
struct S { seastar::future<> stop(){ return seastar::make_ready_future<>(); } };
seastar::sharded<S> s; seastar::future<> f(){ return s.start(); }
