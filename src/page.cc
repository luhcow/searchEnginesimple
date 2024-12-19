#include "Page/Page.hpp"

int main() {
  Page::PageLibPreprocessor p(
      {"/home/rings/searchEngine/data/people"});
  p.cutRedundantPage();

  p.buildDict();
  p.storeOnDisk();
}
