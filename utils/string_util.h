#pragma once

#include <sstream>

using namespace std;

namespace memcachedumper {

class StringUtil {
 public:

  // Splits string 'str' into a vector of tokens based on 'delim' and returns
  // them.
  static std::vector<std::string> Split(std::string str, const char* delim) {
    std::vector<std::string> tokens; 
    std::stringstream strstream(str); 
    std::string intermediate;

    // Tokenizing w.r.t. 'delim'. Assuming it's 1 byte. 
    while(getline(strstream, intermediate, delim[0])) {
      tokens.push_back(intermediate); 
    }
    return tokens;
  }

};

} // namespace memcachedumper
