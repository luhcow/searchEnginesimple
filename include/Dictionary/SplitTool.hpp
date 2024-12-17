#ifndef SG_SPLITTOOL_H__
#define SG_SPLITTOOL_H__

#include <string>
#include <vector>

class SplitTool {
 public:
  virtual std::vector<std::string> cut() = 0;
};

#endif