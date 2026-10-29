#include <ankerl/unordered_dense.h>
#include <string>
ankerl::unordered_dense::map<std::string,int> m; int f(){ m["a"]=1; auto it=m.find("a"); m.erase(it); return m.size(); }
