#include <map>
#include <string>
std::map<std::string,int> m; int f(){ m["a"]=1; auto it=m.find("a"); m.erase(it); return m.size(); }
