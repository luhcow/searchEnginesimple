#ifndef SG_KEYRECOMMANDER_H__
#define SG_KEYRECOMMANDER_H__

#include <fmt/core.h>
#include <fmt/ranges.h>
#include <leveldb/db.h>

#include <cctype>
#include <functional>
#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "json.hpp"
#include "utf8.h"

class KeyRecommander {
 public:
  KeyRecommander(
      std::function<void(std::vector<std::pair<std::string, int>>&,
                         std::map<int, std::set<int>>&)> load) {
    load(dict_, index_);

    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(
        options, "/home/rings/searchEnginesimple/data/testdb", &db);
    assert(status.ok());
  }

  void queryIndexTable(int c, std::set<int>& index) {
    std::string js;
    db->Get(leveldb::ReadOptions(), std::to_string(c), &js);
    nlohmann::json json;
    json = nlohmann::json::parse(js);
    std::set<int> get_set = json;

    index.insert(get_set.begin(), get_set.end());
  }

  int utf8_edit_distance(const std::string& str1,
                         const std::string& str2) {
    // 使用 utf8cpp 提取 UTF-8 字符
    std::vector<std::u32string::value_type> chars1, chars2;

    // 将 UTF-8 字符串解析为 Unicode 码点序列
    utf8::utf8to32(
        str1.begin(), str1.end(), std::back_inserter(chars1));
    utf8::utf8to32(
        str2.begin(), str2.end(), std::back_inserter(chars2));

    size_t len1 = chars1.size();
    size_t len2 = chars2.size();

    // 动态规划表
    std::vector<std::vector<int>> dp(len1 + 1,
                                     std::vector<int>(len2 + 1, 0));

    // 初始化边界情况
    for (size_t i = 0; i <= len1; ++i) dp[i][0] = i;  // 删除所有字符
    for (size_t j = 0; j <= len2; ++j) dp[0][j] = j;  // 插入所有字符

    // 填充 DP 表
    for (size_t i = 1; i <= len1; ++i) {
      for (size_t j = 1; j <= len2; ++j) {
        if (chars1[i - 1] == chars2[j - 1]) {
          dp[i][j] = dp[i - 1][j - 1];  // 相同字符，无需操作
        } else {
          dp[i][j] = std::min(dp[i - 1][j] + 1,           // 删除
                              std::min(dp[i][j - 1] + 1,  // 插入
                                       dp[i - 1][j - 1] + 1)  // 替换
          );
        }
      }
    }
    return dp[len1][len2];  // 返回最终的编辑距离
  }

  std::vector<std::string> statistic(std::string& word,
                                     std::set<int>& index) {
    std::function<bool(std::pair<std::string, int>&,
                       std::pair<std::string, int>&)>
        com = [](std::pair<std::string, int>& lhs,
                 std::pair<std::string, int>& rhs) -> bool {
      return lhs.second > rhs.second;
    };
    std::priority_queue<
        std::pair<std::string, int>,
        std::vector<std::pair<std::string, int>>,
        std::function<bool(std::pair<std::string, int>&,
                           std::pair<std::string, int>&)>>
        que(com);
    for (int i : index) {
      que.push(
          {dict_[i].first, utf8_edit_distance(word, dict_[i].first)});
    }
    std::vector<std::string> reword;

    for (int i = 0; i < std::min(10, (int)que.size()); i++) {
      reword.push_back(que.top().first);
      que.pop();
    }

    return reword;
  }

  nlohmann::json execute(std::string word) {
    for (char& c : word) {
      c = std::tolower(c);
    }
    utf8::iterator<std::string::iterator> beg(
        word.begin(), word.begin(), word.end());
    utf8::iterator<std::string::iterator> end(
        word.end(), word.begin(), word.end());

    std::set<int> index;

    for (; beg != end; beg++) {
      queryIndexTable(*beg, index);
    }

    auto ret = statistic(word, index);

    nlohmann::json json;

    json["data"] = ret;
    return json;
  }

  void close() { delete db; }

 private:
  std::vector<std::pair<std::string, int>> dict_;
  std::map<int, std::set<int>> index_;
  leveldb::DB* db;
};

#endif
