
#include <fcntl.h>
#include <unistd.h>

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

class Command {
 public:
  virtual std::string getKey() const = 0;
  virtual ~Command() = default;
  virtual json toJson() const = 0;
};

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
    json json;
    json["type"] = "SetCommand";
    json["key"] = key;
    json["value"] = value;
    return json;
  }
};

class RmCommand : public Command {
 private:
  std::string key;

 public:
  RmCommand(const std::string& key) : key(key) {}
  std::string getKey() const override { return key; }
  json toJson() const override {
    json json;
    json["type"] = "RmCommand";
    json["key"] = key;
    return json;
  };
};

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

class TableMetaInfo {
 private:
  long dataStart;
  long dataLen;
  long indexStart;
  long indexLen;
  long partSize;  // 分段大小
  long version;

 public:
  TableMetaInfo(long part_size = 0)
      : partSize(part_size),
        dataStart(0),
        dataLen(0),
        indexStart(0),
        indexLen(0),
        version(0) {}

  // 设置和获取字段的方法
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

  /**
   * 将元信息写入文件
   * @param file 输出文件流
   */
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
      throw std::runtime_error(e.what());
    }
  }

  /**
   * 从文件中读取元信息，按照写入顺序倒序读取
   * @param file 输入文件流
   * @return TableMetaInfo对象
   */
  static TableMetaInfo readFromFile(std::fstream& file) {
    try {
      TableMetaInfo tableMetaInfo;
      file.seekg(0, std::ios::end);
      long fileLen = file.tellg();

      file.seekg(fileLen - 8, std::ios::beg);
      long version;
      file.read(reinterpret_cast<char*>(&version), sizeof(version));
      tableMetaInfo.setVersion(version);

      file.seekg(fileLen - 8 * 2, std::ios::beg);
      long indexLen;
      file.read(reinterpret_cast<char*>(&indexLen), sizeof(indexLen));
      tableMetaInfo.setIndexLen(indexLen);

      file.seekg(fileLen - 8 * 3, std::ios::beg);
      long indexStart;
      file.read(reinterpret_cast<char*>(&indexStart),
                sizeof(indexStart));
      tableMetaInfo.setIndexStart(indexStart);

      file.seekg(fileLen - 8 * 4, std::ios::beg);
      long dataLen;
      file.read(reinterpret_cast<char*>(&dataLen), sizeof(dataLen));
      tableMetaInfo.setDataLen(dataLen);

      file.seekg(fileLen - 8 * 5, std::ios::beg);
      long dataStart;
      file.read(reinterpret_cast<char*>(&dataStart),
                sizeof(dataStart));
      tableMetaInfo.setDataStart(dataStart);

      file.seekg(fileLen - 8 * 6, std::ios::beg);
      long partSize;
      file.read(reinterpret_cast<char*>(&partSize), sizeof(partSize));
      tableMetaInfo.setPartSize(partSize);

      return tableMetaInfo;
    } catch (const std::exception& e) {
      throw std::runtime_error(e.what());
    }
  }
};

// 伪代码：Position类定义
struct Position {
  long start;
  long len;

  long getStart() const { return start; }
  long getLen() const { return len; }

  bool operator==(const Position& other) const {
    return start == other.start && len == other.len;
  }

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(Position, start, len)
};

class SsTable {
 private:
  TableMetaInfo tableMetaInfo;
  std::fstream tableFile;
  std::string filePath;
  std::map<std::string, Position> sparseIndex;

  void writeDataPart(const json& partData) {
    std::string data = partData.dump();
    long start = tableFile.tellp();

    tableFile << data;
    if (!partData.empty()) {
      std::string firstKey = partData.begin().key();
      Position pos{start, static_cast<long>(data.length())};
      sparseIndex[firstKey] = pos;
    }
  }

 public:
  SsTable(const std::string& path, int partSize)
      : tableMetaInfo(partSize), filePath(path) {
    if (!std::filesystem::exists(path)) {
      std::ofstream createFile(path);  // 创建空文件
      createFile.close();
    }
    tableFile.open(path);
    if (!tableFile) {
      throw std::runtime_error("Failed to open file: " + path);
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

  /**
   * 从内存表转化为ssTable
   * @param index
   */
  void initFromIndex(
      const std::map<std::string, std::shared_ptr<Command>>& index) {
    try {
      json partData = json::object();
      tableMetaInfo.setDataStart(tableFile.tellp());

      for (const auto& [key, command] : index) {
        if (command) {
          partData[key] = command->toJson();
        }

        // 达到分段数量，写入数据段
        if (partData.size() >= tableMetaInfo.getPartSize()) {
          writeDataPart(partData);
          partData.clear();
        }
      }

      // 遍历完之后如果有剩余的数据（尾部数据不一定达到分段条件）写入文件
      if (!partData.empty()) {
        writeDataPart(partData);
      }

      // 记录数据段总长度
      long dataPartLen =
          tableFile.tellp() - tableMetaInfo.getDataStart();
      tableMetaInfo.setDataLen(dataPartLen);

      // 保存稀疏索引
      json jt = sparseIndex;
      std::string indexStr = jt.dump();
      tableMetaInfo.setIndexStart(tableFile.tellp());
      tableFile.write(indexStr.c_str(), indexStr.size());
      tableMetaInfo.setIndexLen(indexStr.size());

      // 保存元信息
      tableMetaInfo.writeToFile(tableFile);

      // 日志输出（伪代码）
      // std::cout << "[SsTable][initFromIndex]: " << filePath << ",
      // "
      // << tableMetaInfo << std::endl;

    } catch (const std::exception& e) {
      throw std::runtime_error(e.what());
    }
  }

  /**
   * 从文件中恢复SSTable到内存
   */
  void restoreFromFile() {
    try {
      // 读取元信息
      TableMetaInfo tableMetaInfo =
          TableMetaInfo::readFromFile(tableFile);

      // 伪代码：输出调试信息
      // std::cout << "[SsTable][restoreFromFile][tableMetaInfo]: "
      // << tableMetaInfo << std::endl;

      // 读取稀疏索引
      tableFile.seekg(tableMetaInfo.getIndexStart(), std::ios::beg);
      std::vector<char> indexBytes(tableMetaInfo.getIndexLen());
      tableFile.read(indexBytes.data(), tableMetaInfo.getIndexLen());
      std::string indexStr(indexBytes.begin(), indexBytes.end());

      // 伪代码：输出调试信息
      // std::cout << "[SsTable][restoreFromFile][indexStr]: " <<
      // indexStr << std::endl;

      // 将索引字符串解析为稀疏索引
      json indexJson = json::parse(indexStr);
      sparseIndex = indexJson.get<std::map<std::string, Position>>();

      this->tableMetaInfo = tableMetaInfo;

      std::cerr << "[SsTable][restoreFromFile][sparseIndex]: "
                << indexJson.dump() << std::endl;

    } catch (const std::exception& e) {
      throw std::runtime_error(e.what());
    }
  }

  /**
   * 从SSTable中查询数据
   * @param key 查询的键
   * @return 返回Command对象或空指针
   */
  std::unique_ptr<Command> query(const std::string& key) {
    try {
      std::list<Position> sparseKeyPositionList;

      Position lastSmallPosition;
      Position firstBigPosition;
      bool hasLastSmall = false;
      bool hasFirstBig = false;

      // 从稀疏索引中找到最后一个小于等于key的位置，以及第一个大于key的位置
      for (const auto& [k, pos] : sparseIndex) {
        if (k <= key) {
          lastSmallPosition = pos;
          hasLastSmall = true;
        } else {
          firstBigPosition = pos;
          hasFirstBig = true;
          break;
        }
      }

      if (hasLastSmall) {
        sparseKeyPositionList.push_back(lastSmallPosition);
      }
      if (hasFirstBig) {
        sparseKeyPositionList.push_back(firstBigPosition);
      }
      if (sparseKeyPositionList.empty()) {
        return nullptr;
      }

      // 伪代码：调试日志
      // std::cout << "[SsTable][query][sparseKeyPositionList]: " <<
      // sparseKeyPositionList << std::endl;

      auto firstKeyPosition = sparseKeyPositionList.front();
      auto lastKeyPosition = sparseKeyPositionList.back();
      long start = firstKeyPosition.getStart();
      long len = 0;
      if (firstKeyPosition == lastKeyPosition) {
        len = firstKeyPosition.getLen();
      } else {
        len = (lastKeyPosition.getStart() + lastKeyPosition.getLen() -
               start);
      }

      // key如果存在必定位于区间内，所以只需要读取区间内的数据，减少io
      std::vector<char> dataPart(len);
      tableFile.seekg(start, std::ios::beg);
      tableFile.read(dataPart.data(), len);

      int pStart = 0;
      // 遍历分区数据
      for (const auto& position : sparseKeyPositionList) {
        json dataPartJson = json::parse(std::string(
            dataPart.begin() + pStart,
            dataPart.begin() + pStart + position.getLen()));

        // 伪代码：调试日志
        // std::cout << "[SsTable][query][dataPartJson]: " <<
        // dataPartJson << std::endl;

        if (dataPartJson.contains(key)) {
          auto value = dataPartJson[key];

          // 伪代码：将JSON对象转换为Command对象
          return ConvertUtil::jsonToCommand(value);
        }
        pStart += position.getLen();
      }
      return nullptr;
    } catch (const std::exception& e) {
      throw std::runtime_error(e.what());
    }
  }
};

// LsmKvStore类定义
class LsmKvStore {
 private:
  std::string dataDir;
  int storeThreshold;
  long partSize;
  std::shared_mutex indexMutex;
  std::list<std::shared_ptr<SsTable>> ssTables;
  std::shared_ptr<std::fstream> wal;  // 使用智能指针管理WAL文件
  std::filesystem::path walFile;

  std::map<std::string, std::shared_ptr<Command>> index;
  std::map<std::string, std::shared_ptr<Command>> immutableIndex;

  static constexpr const char* WAL = "wal";
  static constexpr const char* WAL_TMP = "walTmp";
  static constexpr const char* TABLE = ".sstable";
  static constexpr const char* RW_MODE = "rw";

  // 伪代码：从WAL恢复的方法
  void restoreFromWal(std::shared_ptr<std::fstream> walFile) {
    // 伪代码：从WAL文件中恢复数据到内存索引
  }

 public:
  /**
   * 初始化
   * @param dataDir 数据目录
   * @param storeThreshold 持久化阈值
   * @param partSize 数据分区大小
   */
  LsmKvStore(const std::string& dataDir,
             int storeThreshold,
             int partSize)
      : dataDir(dataDir),
        storeThreshold(storeThreshold),
        partSize(partSize) {
    try {
      ssTables = std::list<std::shared_ptr<SsTable>>();
      index = std::map<std::string, std::shared_ptr<Command>>();

      // 检查目录是否存在
      if (!std::filesystem::exists(dataDir) ||
          !std::filesystem::is_directory(dataDir)) {
        throw std::runtime_error(
            "Data directory does not exist or is not a directory.");
      }

      // 列出数据目录中的所有文件
      std::vector<std::filesystem::path> files;
      for (const auto& entry :
           std::filesystem::directory_iterator(dataDir)) {
        if (std::filesystem::is_regular_file(entry.path())) {
          files.push_back(entry.path());
        }
      }

      // 如果目录为空，初始化WAL文件
      if (files.empty()) {
        walFile = std::filesystem::path(dataDir) / WAL;
        wal = std::make_shared<std::fstream>(
            walFile,
            std::ios::in | std::ios::out | std::ios::binary |
                std::ios::trunc);
        return;
      }

      // 从大到小加载SsTable
      std::map<long, std::shared_ptr<SsTable>, std::greater<>>
          ssTableTreeMap;
      for (const auto& file : files) {
        std::string fileName = file.filename().string();

        // 处理WAL_TMP文件，恢复数据
        if (fileName == WAL_TMP) {
          restoreFromWal(std::make_shared<std::fstream>(
              file, std::ios::in | std::ios::out | std::ios::binary));
        }

        // 加载SsTable
        if (fileName.rfind(TABLE) != std::string::npos) {
          size_t dotIndex = fileName.find(".");
          long time = std::stol(fileName.substr(0, dotIndex));
          ssTableTreeMap[time] =
              SsTable::createFromFile(file.string());
        } else if (fileName == WAL) {
          // 加载WAL文件
          walFile = file;
          wal = std::make_shared<std::fstream>(
              file, std::ios::in | std::ios::out | std::ios::binary);
          restoreFromWal(wal);
        }
      }

      // 添加所有的SsTable到列表
      for (const auto& [time, table] : ssTableTreeMap) {
        ssTables.push_back(table);
      }
    } catch (const std::exception& e) {
      throw std::runtime_error(e.what());
    }
  }

  /**
   * 设置键值对
   * @param key 键
   * @param value 值
   */
  void set(const std::string& key, const std::string& value) {
    try {
      // 构造SetCommand
      auto command = std::make_shared<SetCommand>(key, value);
      json commandJson = command->toJson();
      std::string commandStr = commandJson.dump();
      int commandSize = static_cast<int>(commandStr.size());

      // 上写锁
      std::unique_lock lock(indexMutex);

      // 写入WAL
      wal->write(reinterpret_cast<const char*>(&commandSize),
                 sizeof(commandSize));
      wal->write(commandStr.c_str(), commandSize);
      wal->flush();

      // 更新内存索引
      index[key] = command;

      // 如果内存表大小超过阈值，进行持久化
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

  /**
   * 获取指定key的值
   * @param key 查询的键
   * @return 返回对应的值，如果不存在返回空字符串
   */
  std::string get(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(indexMutex);  // 加读锁

    try {
      // 先从内存表中查找
      auto it = index.find(key);
      std::shared_ptr<Command> command =
          (it != index.end()) ? it->second : nullptr;

      // 如果在不可变内存表中查找
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

      // 如果都找不到，返回null
      return "";
    } catch (const std::exception& e) {
      throw std::runtime_error(e.what());
    }
  }

 private:
  /**
   * 切换内存表，新建一个内存表，老的暂存起来
   */
  void switchIndex() {
    try {
      // 切换内存表
      immutableIndex = std::move(index);
      index = std::map<std::string, std::shared_ptr<Command>>();

      wal->close();

      // 切换WAL文件
      std::filesystem::path tmpWal =
          std::filesystem::path(dataDir) / WAL_TMP;
      if (std::filesystem::exists(tmpWal)) {
        if (!std::filesystem::remove(tmpWal)) {
          throw std::runtime_error("删除文件失败: walTmp");
        }
      }
      std::filesystem::rename(walFile, tmpWal);

      walFile = std::filesystem::path(dataDir) / WAL;
      wal = std::make_shared<std::fstream>(
          walFile,
          std::ios::in | std::ios::out | std::ios::binary |
              std::ios::trunc);

      if (!wal->is_open()) {
        throw std::runtime_error("Failed to reopen WAL file: " +
                                 walFile.string());
      }

    } catch (const std::exception& e) {
      throw std::runtime_error(e.what());
    }
  }

  /**
   * 保存数据到SsTable
   */
  void storeToSsTable() {
    try {
      // 创建新的SsTable文件名
      std::string tableName =
          std::to_string(std::chrono::system_clock::now()
                             .time_since_epoch()
                             .count());
      std::string tablePath =
          (std::filesystem::path(dataDir) / (tableName + TABLE))
              .string();

      // 创建新的SsTable
      auto ssTable = SsTable::createFromIndex(
          tablePath, partSize, immutableIndex);
      ssTables.push_front(ssTable);

      // 清理
      immutableIndex.clear();
      std::filesystem::path tmpWal =
          std::filesystem::path(dataDir) / WAL_TMP;
      if (std::filesystem::exists(tmpWal)) {
        std::filesystem::remove(tmpWal);
      }
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to store to SSTable: " +
                               std::string(e.what()));
    }
  }
};

int main() {
  LsmKvStore lsm("/home/rings/searchEngine/wal", 5, 5);
  for (int i = 0; i < 10; i++) {
    lsm.set("key2" + std::to_string(i), "value" + std::to_string(i));
  }

  std::cerr << lsm.get("key1");
}