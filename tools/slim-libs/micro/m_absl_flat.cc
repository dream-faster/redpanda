#include <absl/container/flat_hash_map.h>
#include <string>
absl::flat_hash_map<std::string,int> m; int f(){ m["a"]=1; auto it=m.find("a"); m.erase(it); return m.size(); }
