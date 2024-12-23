#include <iostream>

#include "Page/WebPageQuery.hpp"

int main() {
  WebPageQuery wpq("/home/rings/searchEnginesimple/data/page_db");
  std::cerr
      << wpq.executeQuery(
                "在新能源车成为全球汽车产业和消费转型升级主要方向")
             .dump();
}