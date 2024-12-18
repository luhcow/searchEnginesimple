#include "Dictionary/Dictionary.hpp"


int main() {
  dictionary::DictProducer dicp(
      "/home/rings/searchEngine/data/files.json");
  dicp.buildDict();
  dicp.creatIndex();
  dicp.store();
}