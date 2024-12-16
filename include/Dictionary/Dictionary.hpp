#ifndef SG_DICTPRODUCER_H__
#define SG_DICTPRODUCER_H__

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "Dictionary/SplitTool.hpp"
#include "Page/Configuration.hpp"

namespace dictionary {

class SplitToolCppJieba {
 public:
  std::vector<std::string> cut() {
  }
};

class DictProducer {
 public:
  DictProducer(std::string a /*, tool b*/) {
  }
  void buildEnDict() {
  }
  void buildCnDict() {
  }
  void creatIndex() {
  }
  void store() {
  }

 private:
  std::vector<std::string> files_;
  std::vector<std::pair<std::string, int>> dict_;
  std::map<std::string, std::set<int>> index_;
  SplitTool* cuttor_;
};

}  // namespace dictionary
#endif