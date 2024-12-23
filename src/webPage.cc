#include "Dictionary/Dictionary.hpp"

int main() {
  dictionary::DictProducer dicp(
      "/home/rings/searchEnginesimple/data/files.json");
  dicp.buildDict();
  dicp.creatIndex();
  dicp.store();
}