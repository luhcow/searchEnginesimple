#ifndef SG_READALL_H__
#define SG_READALL_H__

#include <algorithm>
#include <fstream>
#include <string>

class ReadAll {
 public:
  static const std::string& read(std::string path) {
    std::ifstream ifs(path);
    std::istreambuf_iterator<char> beg(ifs), end;
    std::string s2(beg, end);
    return std::move(s2);
  }
};

#endif