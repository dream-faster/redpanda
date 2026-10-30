#include <boost/lexical_cast.hpp>
int f(){ return boost::lexical_cast<int>(std::string("12")); }
