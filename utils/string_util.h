/**
 *
 *  Copyright 2018 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

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
