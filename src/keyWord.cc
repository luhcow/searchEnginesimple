#include "Dictionary/Dictionary.hpp"

int main() {
  dictionary::DictProducer dicp(
      "/home/rings/searchEngine/conf/files.json");
  dicp.buildDict();
  dicp.creatIndex();
  dicp.store();
}