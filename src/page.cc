#include "Page/Page.hpp"

int main() {
  Page::PageLibPreprocessor p(
      {"/home/rings/searchEnginesimple/corpus/people"});
  p.cutRedundantPage();

  p.buildDict();
  p.storeOnDisk();
  p.close_fd();
}
