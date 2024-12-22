#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "json.hpp"  // 使用nlohmann/json库
using json = nlohmann::json;
namespace fs = std::filesystem;

// 基础Command类
class Command {
 public:
  virtual std::string getKey() const = 0;
  virtual json toJson() const = 0;
  virtual ~Command() = default;
};

// SetCommand实现
class SetCommand : public Command {
 private:
  std::string key;
  std::string value;

 public:
  SetCommand(const std::string& key, const std::string& value)
      : key(key), value(value) {}

  std::string getKey() const override { return key; }
  std::string getValue() const { return value; }

  json toJson() const override {
    return {{"type", "SetCommand"}, {"key", key}, {"value", value}};
  }
};

// RmCommand实现
class RmCommand : public Command {
 private:
  std::string key;

 public:
  explicit RmCommand(const std::string& key) : key(key) {}

  std::string getKey() const override { return key; }

  json toJson() const override {
    return {{"type", "RmCommand"}, {"key", key}};
  }
};

// 工具类实现
class ConvertUtil {
 public:
  static std::unique_ptr<Command> jsonToCommand(const json& j) {
    std::string type = j["type"];
    if (type == "SetCommand") {
      return std::make_unique<SetCommand>(
          j["key"].get<std::string>(), j["value"].get<std::string>());
    } else if (type == "RmCommand") {
      return std::make_unique<RmCommand>(j["key"].get<std::string>());
    }
    return nullptr;
  }
};

// 表元信息类
class TableMetaInfo {
 private:
  long dataStart;
  long dataLen;
  long indexStart;
  long indexLen;
  long partSize;
  long version;

 public:
  explicit TableMetaInfo(long part_size = 0)
      : partSize(part_size),
        dataStart(0),
        dataLen(0),
        indexStart(0),
        indexLen(0),
        version(0) {}

  // Getters and setters
  void setPartSize(long size) { partSize = size; }
  void setDataStart(long start) { dataStart = start; }
  void setDataLen(long len) { dataLen = len; }
  void setIndexStart(long start) { indexStart = start; }
  void setIndexLen(long len) { indexLen = len; }
  void setVersion(long ver) { version = ver; }

  long getPartSize() const { return partSize; }
  long getDataStart() const { return dataStart; }
  long getDataLen() const { return dataLen; }
  long getIndexStart() const { return indexStart; }
  long getIndexLen() const { return indexLen; }
  long getVersion() const { return version; }

  void writeToFile(std::fstream& file) {
    try {
      file.write(reinterpret_cast<const char*>(&partSize),
                 sizeof(partSize));
      file.write(reinterpret_cast<const char*>(&dataStart),
                 sizeof(dataStart));
      file.write(reinterpret_cast<const char*>(&dataLen),
                 sizeof(dataLen));
      file.write(reinterpret_cast<const char*>(&indexStart),
                 sizeof(indexStart));
      file.write(reinterpret_cast<const char*>(&indexLen),
                 sizeof(indexLen));
      file.write(reinterpret_cast<const char*>(&version),
                 sizeof(version));
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to write meta info: " +
                               std::string(e.what()));
    }
  }

  static TableMetaInfo readFromFile(std::fstream& file) {
    try {
      TableMetaInfo metaInfo;
      file.seekg(0, std::ios::end);
      long fileLen = file.tellg();

      // 读取所有字段
      file.seekg(fileLen - sizeof(long), std::ios::beg);
      file.read(reinterpret_cast<char*>(&metaInfo.version),
                sizeof(long));

      file.seekg(fileLen - 2 * sizeof(long), std::ios::beg);
      file.read(reinterpret_cast<char*>(&metaInfo.indexLen),
                sizeof(long));

      file.seekg(fileLen - 3 * sizeof(long), std::ios::beg);
      file.read(reinterpret_cast<char*>(&metaInfo.indexStart),
                sizeof(long));

      file.seekg(fileLen - 4 * sizeof(long), std::ios::beg);
      file.read(reinterpret_cast<char*>(&metaInfo.dataLen),
                sizeof(long));

      file.seekg(fileLen - 5 * sizeof(long), std::ios::beg);
      file.read(reinterpret_cast<char*>(&metaInfo.dataStart),
                sizeof(long));

      file.seekg(fileLen - 6 * sizeof(long), std::ios::beg);
      file.read(reinterpret_cast<char*>(&metaInfo.partSize),
                sizeof(long));

      return metaInfo;
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to read meta info: " +
                               std::string(e.what()));
    }
  }
};

// Position结构体
struct Position {
  long start;
  long len;

  long getStart() const { return start; }
  long getLen() const { return len; }

  bool operator==(const Position& other) const {
    return start == other.start && len == other.len;
  }

  static Position fromJson(const json& j) {
    Position pos;
    pos.start = j["start"];
    pos.len = j["len"];
    return pos;
  }

  json toJson() const { return {{"start", start}, {"len", len}}; }
};

// SSTable实现
class SsTable {
 private:
  TableMetaInfo tableMetaInfo;
  std::fstream tableFile;
  std::string filePath;
  std::map<std::string, Position> sparseIndex;

  void writeDataPart(const json& partData) {
    std::string data = partData.dump();
    long start = tableFile.tellp();
    tableFile.write(data.c_str(), data.length());

    if (!partData.empty()) {
      std::string firstKey = partData.begin().key();
      Position pos{start, static_cast<long>(data.length())};
      sparseIndex[firstKey] = pos;
    }
  }

 public:
  SsTable(const std::string& path, int partSize)
      : tableMetaInfo(partSize),
        filePath(path),
        tableFile(path,
                  std::ios::binary | std::ios::in | std::ios::out |
                      std::ios::trunc) {
    if (!tableFile.is_open()) {
      throw std::runtime_error("Failed to open table file: " + path);
    }
  }

  ~SsTable() {
    if (tableFile.is_open()) {
      tableFile.close();
    }
  }

  static std::shared_ptr<SsTable> createFromFile(
      const std::string& path) {
    auto table = std::make_shared<SsTable>(path, 0);
    table->restoreFromFile();
    return table;
  }

  static std::shared_ptr<SsTable> createFromIndex(
      const std::string& path,
      long partSize,
      const std::map<std::string, std::shared_ptr<Command>>& index) {
    auto table = std::make_shared<SsTable>(path, partSize);
    table->initFromIndex(index);
    return table;
  }

  void initFromIndex(
      const std::map<std::string, std::shared_ptr<Command>>& index) {
    try {
      json partData = json::object();
      tableMetaInfo.setDataStart(tableFile.tellp());

      for (const auto& [key, command] : index) {
        if (command) {
          partData[key] = command->toJson();
        }

        if (partData.size() >= tableMetaInfo.getPartSize()) {
          writeDataPart(partData);
          partData.clear();
        }
      }

      if (!partData.empty()) {
        writeDataPart(partData);
      }

      long dataPartLen =
          tableFile.tellp() - tableMetaInfo.getDataStart();
      tableMetaInfo.setDataLen(dataPartLen);

      json indexJson;
      for (const auto& [key, pos] : sparseIndex) {
        indexJson[key] = pos.toJson();
      }
      std::string indexStr = indexJson.dump();

      tableMetaInfo.setIndexStart(tableFile.tellp());
      tableFile.write(indexStr.c_str(), indexStr.length());
      tableMetaInfo.setIndexLen(indexStr.length());

      tableMetaInfo.writeToFile(tableFile);
      tableFile.flush();

    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to initialize from index: " +
                               std::string(e.what()));
    }
  }

  void restoreFromFile() {
    try {
      tableFile.seekg(0, std::ios::end);
      if (tableFile.tellg() < 6 * sizeof(long)) {
        throw std::runtime_error("Invalid file size");
      }

      tableMetaInfo = TableMetaInfo::readFromFile(tableFile);

      tableFile.seekg(tableMetaInfo.getIndexStart());
      std::vector<char> indexBytes(tableMetaInfo.getIndexLen());
      tableFile.read(indexBytes.data(), tableMetaInfo.getIndexLen());
      std::string indexStr(indexBytes.begin(), indexBytes.end());

      json indexJson = json::parse(indexStr);
      for (const auto& [key, value] : indexJson.items()) {
        sparseIndex[key] = Position::fromJson(value);
      }

    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to restore from file: " +
                               std::string(e.what()));
    }
  }

  std::unique_ptr<Command> query(const std::string& key) {
    try {
      std::list<Position> positions;
      auto it = sparseIndex.lower_bound(key);

      if (it != sparseIndex.begin()) {
        --it;
        positions.push_back(it->second);
      }

      it = sparseIndex.upper_bound(key);
      if (it != sparseIndex.end()) {
        positions.push_back(it->second);
      }

      if (positions.empty()) {
        return nullptr;
      }

      auto& firstPos = positions.front();
      auto& lastPos = positions.back();

      long start = firstPos.getStart();
      long len =
          (firstPos == lastPos)
              ? firstPos.getLen()
              : (lastPos.getStart() + lastPos.getLen() - start);

      std::vector<char> dataPart(len);
      tableFile.seekg(start);
      tableFile.read(dataPart.data(), len);

      int pStart = 0;
      for (const auto& pos : positions) {
        std::string dataStr(dataPart.begin() + pStart,
                            dataPart.begin() + pStart + pos.getLen());
        json dataJson = json::parse(dataStr);

        if (dataJson.contains(key)) {
          return ConvertUtil::jsonToCommand(dataJson[key]);
        }
        pStart += pos.getLen();
      }

      return nullptr;
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to query: " +
                               std::string(e.what()));
    }
  }
};

// LSM KV Store实现
class LsmKvStore {
 private:
  std::string dataDir;
  int storeThreshold;
  long partSize;
  std::shared_mutex indexMutex;
  std::list<std::shared_ptr<SsTable>> ssTables;
  std::shared_ptr<std::fstream> wal;
  fs::path walFile;
  std::map<std::string, std::shared_ptr<Command>> index;
  std::map<std::string, std::shared_ptr<Command>> immutableIndex;

  static constexpr const char* WAL = "/wal";
  static constexpr const char* WAL_TMP = "/walTmp";
  static constexpr const char* TABLE = ".sstable";

  void restoreFromWal(std::shared_ptr<std::fstream> walFile) {
    try {
      walFile->seekg(0, std::ios::beg);
      while (walFile->good() && !walFile->eof()) {
        int commandSize;
        walFile->read(reinterpret_cast<char*>(&commandSize),
                      sizeof(commandSize));

        if (walFile->eof()) break;

        std::vector<char> commandData(commandSize);
        walFile->read(commandData.data(), commandSize);

        if (walFile->gcount() != commandSize) break;

        std::string commandStr(commandData.begin(),
                               commandData.end());
        json commandJson = json::parse(commandStr);
        auto command = ConvertUtil::jsonToCommand(commandJson);

        if (command) {
          index[command->getKey()] = std::move(command);
        }
      }
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to restore from WAL: " +
                               std::string(e.what()));
    }
  }

 public:
  LsmKvStore(const std::string& dataDir,
             int storeThreshold,
             int partSize)
      : dataDir(dataDir),
        storeThreshold(storeThreshold),
        partSize(partSize) {
    try {
      if (!fs::exists(dataDir) || !fs::is_directory(dataDir)) {
        if (!fs::create_directories(dataDir)) {
          throw std::runtime_error(
              "Failed to create data directory: " + dataDir);
        }
      }

      std::vector<fs::path> files;
      for (const auto& entry : fs::directory_iterator(dataDir)) {
        if (fs::is_regular_file(entry.path())) {
          files.push_back(entry.path());
        }
      }

      if (files.empty()) {
        walFile = fs::path(dataDir) / WAL;
        wal = std::make_shared<std::fstream>(
            walFile,
            std::ios::in | std::ios::out | std::ios::binary |
                std::ios::trunc);
        if (!wal->is_open()) {
          throw std::runtime_error("Failed to create WAL file: " +
                                   walFile.string());
        }
        return;
      }

      // 从大到小加载SsTable
      std::map<long, std::shared_ptr<SsTable>, std::greater<>>
          ssTableMap;
      for (const auto& file : files) {
        std::string fileName = file.filename().string();

        if (fileName == WAL_TMP) {
          auto tmpWal = std::make_shared<std::fstream>(
              file, std::ios::in | std::ios::out | std::ios::binary);
          if (tmpWal->is_open()) {
            restoreFromWal(tmpWal);
          }
        } else if (fileName.rfind(TABLE) != std::string) {
          size_t dotIndex = fileName.find('.');
          if (dotIndex != std::string::npos) {
            long time = std::stol(fileName.substr(0, dotIndex));
            ssTableMap[time] = SsTable::createFromFile(file.string());
          }
        } else if (fileName == WAL) {
          walFile = file;
          wal = std::make_shared<std::fstream>(
              file, std::ios::in | std::ios::out | std::ios::binary);
          if (wal->is_open()) {
            restoreFromWal(wal);
          }
        }
      }

      // 添加所有的SsTable到列表
      for (const auto& [_, table] : ssTableMap) {
        ssTables.push_back(table);
      }

    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to initialize LsmKvStore: " +
                               std::string(e.what()));
    }
  }

  void set(const std::string& key, const std::string& value) {
    try {
      auto command = std::make_shared<SetCommand>(key, value);
      json commandJson = command->toJson();
      std::string commandStr = commandJson.dump();
      int commandSize = static_cast<int>(commandStr.size());

      std::unique_lock<std::shared_mutex> lock(indexMutex);

      // 写入WAL
      wal->write(reinterpret_cast<const char*>(&commandSize),
                 sizeof(commandSize));
      wal->write(commandStr.c_str(), commandSize);
      wal->flush();

      // 更新内存索引
      index[key] = command;

      // 检查是否需要持久化
      if (index.size() > storeThreshold) {
        switchIndex();
        storeToSsTable();
      }
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to set key-value: " +
                               std::string(e.what()));
    }
  }

  void remove(const std::string& key) {
    try {
      auto command = std::make_shared<RmCommand>(key);
      json commandJson = command->toJson();
      std::string commandStr = commandJson.dump();
      int commandSize = static_cast<int>(commandStr.size());

      std::unique_lock<std::shared_mutex> lock(indexMutex);

      // 写入WAL
      wal->write(reinterpret_cast<const char*>(&commandSize),
                 sizeof(commandSize));
      wal->write(commandStr.c_str(), commandSize);
      wal->flush();

      // 更新内存索引
      index[key] = command;

      // 检查是否需要持久化
      if (index.size() > storeThreshold) {
        switchIndex();
        storeToSsTable();
      }
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to remove key: " +
                               std::string(e.what()));
    }
  }

  std::string get(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(indexMutex);

    try {
      // 先从内存表中查找
      auto it = index.find(key);
      std::shared_ptr<Command> command =
          (it != index.end()) ? it->second : nullptr;

      // 从不可变内存表中查找
      if (!command && !immutableIndex.empty()) {
        auto immIt = immutableIndex.find(key);
        command =
            (immIt != immutableIndex.end()) ? immIt->second : nullptr;
      }

      // 从SsTable中查找
      if (!command) {
        for (const auto& ssTable : ssTables) {
          auto result = ssTable->query(key);
          if (result) {
            command = std::move(result);
            break;
          }
        }
      }

      // 处理查询结果
      if (auto setCommand =
              std::dynamic_pointer_cast<SetCommand>(command)) {
        return setCommand->getValue();
      } else if (std::dynamic_pointer_cast<RmCommand>(command)) {
        return "";  // 键已被删除
      }

      return "";  // 键不存在
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to get value: " +
                               std::string(e.what()));
    }
  }

 private:
  void switchIndex() {
    std::unique_lock lock(indexMutex);  // 加写锁
    try {
      immutableIndex = std::move(index);
      index = std::map<std::string, std::shared_ptr<Command>>();

      wal->close();

      fs::path tmpWal = fs::path(dataDir) / WAL_TMP;
      if (fs::exists(tmpWal)) {
        fs::remove(tmpWal);
      }
      fs::rename(walFile, tmpWal);

      walFile = fs::path(dataDir) / WAL;
      wal = std::make_shared<std::fstream>(
          walFile,
          std::ios::in | std::ios::out | std::ios::binary |
              std::ios::trunc);

      if (!wal->is_open()) {
        throw std::runtime_error("Failed to reopen WAL file: " +
                                 walFile.string());
      }
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to switch index: " +
                               std::string(e.what()));
    }
  }

  void storeToSsTable() {
    try {
      // 创建新的SsTable文件名
      std::string tableName =
          std::to_string(std::chrono::system_clock::now()
                             .time_since_epoch()
                             .count());
      std::string tablePath =
          (fs::path(dataDir) / (tableName + TABLE)).string();

      // 创建新的SsTable
      auto ssTable = SsTable::createFromIndex(
          tablePath, partSize, immutableIndex);
      ssTables.push_front(ssTable);

      // 清理
      immutableIndex.clear();
      fs::path tmpWal = fs::path(dataDir) / WAL_TMP;
      if (fs::exists(tmpWal)) {
        fs::remove(tmpWal);
      }
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to store to SSTable: " +
                               std::string(e.what()));
    }
  }
};
