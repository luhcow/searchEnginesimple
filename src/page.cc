#include "Page/Page.hpp"

int main() {
  Page::PageLibPreprocessor p(
      {"/home/rings/searchEngine/data/人民网语料"});
  p.cutRedundantPage();

  p.buildDict();
  p.storeOnDisk();
}
