#include <cassert>

#include "leveldb/db.h"

int main() {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(
      options, "/home/rings/searchEnginesimple/data/testdb", &db);
  assert(status.ok());

  std::string value("hello");

  leveldb::Status s = db->Get(leveldb::ReadOptions(), "key1", &value);
  if (s.ok()) s = db->Put(leveldb::WriteOptions(), "key2", value);
  if (s.ok()) s = db->Delete(leveldb::WriteOptions(), "key1");
  delete db;
}
