#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
std::string f(){ return boost::uuids::to_string(boost::uuids::random_generator()()); }
