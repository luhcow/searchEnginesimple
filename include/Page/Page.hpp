#ifndef SG_PAGE_H__
#define SG_PAGE_H__

#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Dictionary/SplitTool.hpp"
#include "Page/Configuration.hpp"
#include "WebPage.hpp"

namespace Page {

class DirScanner {
 public:
  std::vector<std::string>& get_files() {
  }
  void traverse(std::string dir) {
  }

 private:
  std::vector<std::string> files_;
};

class PageLib {
 public:
  PageLib() {
  }
  void create() {
  }
  void store() {
  }

 private:
  DirScanner dir_scanner_;
  std::vector<std::string> pages_;
  std::map<int, std::pair<int, int>> offset_lib_;
};

class PageLibPreprocessor {
 public:
  void cutRedundantPage() {
  }
  void buildLnvertIndexMap() {
  }
  void storeOnDisk() {
  }

 private:
  std::vector<WebPage> page_list_;
  std::unordered_map<int, std::pair<int, int>> offset_lib_;
  std::unordered_map<int, std::pair<int, int>> invert_index_lib_;
  SplitTool* word_cutter_;
};

}  // namespace Page

#endif