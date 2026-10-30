#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/repeated_field.h>
google::protobuf::RepeatedField<int> r; int f(){ r.Add(1); return r.size(); }
