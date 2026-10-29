#include <google/protobuf/message_lite.h>
#include <google/protobuf/repeated_field.h>
google::protobuf::RepeatedField<int> r; int f(){ r.Add(1); return r.size(); }
