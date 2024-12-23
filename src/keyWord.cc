#include "Dictionary/Dictionary.hpp"

int main() {
  dictionary::DictProducer dicp(
      "/home/rings/searchEnginesimple/conf/files.json");
  dicp.buildDict();
  dicp.creatIndex();
  dicp.store();
}