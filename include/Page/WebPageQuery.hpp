#ifndef SG_WEBPAGEQUERY_H__
#define SG_WEBPAGEQUERY_H__

#include <fcntl.h>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <xapian.h>

#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "Page/WebPage.hpp"
#include "cereal/archives/binary.hpp"
#include "cereal/types/map.hpp"
#include "cereal/types/set.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/vector.hpp"
#include "cppjieba/Jieba.hpp"
#include "json.hpp"
#include "utf8.h"

class WebPageQuery {
 public:
  WebPageQuery(std::string bina) : db_(bina) {
  }

  nlohmann::json executeQuery(std::string sentence) {
    // 对查询关键词进行分词
    std::vector<std::string> query_words;
    jieba_.Cut(sentence, query_words);

    // 将分词结果拼接为以空格分隔的字符串
    std::string query_tokens = "";
    for (const auto& word : query_words) {
      query_tokens += word + " ";
    }

    // 创建 QueryParser
    Xapian::QueryParser query_parser;
    query_parser.add_prefix("title", "S");  // 设置前缀（标题字段）

    // 解析查询
    Xapian::Query query = query_parser.parse_query(query_tokens);

    // 创建 Enquire 对象用于执行查询
    Xapian::Enquire enquire(db_);
    enquire.set_query(query);

    // 获取查询结果（返回前 10 个结果）
    Xapian::MSet matches = enquire.get_mset(0, 10);

    nlohmann::json json;

    for (Xapian::MSetIterator it = matches.begin();
         it != matches.end();
         ++it) {
      nlohmann::json json_one;
      Xapian::Document doc = it.get_document();

      json_one["title"] =
          doc.get_data().substr(0, doc.get_data().find("\n"));
      json_one["link"] = doc.get_value(1);
      json_one["content"] =
          doc.get_data().substr(doc.get_data().find("\n") + 1);

      json.push_back(json_one);
    }
    return json;
  }
  std::map<std::string, double> getQueryWordsWeightVector(
      std::vector<std::string> words) {
    std::map<std::string, int> count;

    for (auto& i : words) {
      auto it = i.begin();
      int cp = utf8::next(it, i.end());
      if (cp <= 0x9fa5 && cp >= 0x4e00) {
        count[i]++;

      } else if ((cp >= 0x0041 && cp <= 0x005a) ||
                 (cp >= 0x0061 && cp <= 0x007a)) {
        for (char& j : i) {
          if (j >= 0x0041 && j <= 0x005a) {
            j = j - 0x20;
          }
        }

        count[i]++;
      }
    }

    // 权重
    double DF = 1.0;
    double IDF = log2(1.0 / (DF + 1));
    std::map<std::string, double> weight;
    for (auto& key : count) {
      auto TF = key.second;
      fmt::print("\nTF is {}\n", TF);
      weight[key.first] = IDF * TF;
      fmt::print("IDF is {}\n", IDF);
      fmt::print("weight[key.first] is {}\n", IDF);
    }

    // 归一化
    double sqr = 0.0;
    for (auto& key : weight) {
      sqr += pow(key.second, 2);
    }
    sqr = sqrt(sqr);
    for (auto& key : weight) {
      key.second /= sqr;
    }

    return weight;
  }

  void close_fd() {
    close(fd_);
  }

 private:
  std::vector<WebPage> page_list_;
  std::map<std::string, int> true_map_;
  std::map<std::string, std::map<int, double>> invert_index_lib_;
  std::map<int, std::map<std::string, int>> page_index_;
  cppjieba::Jieba jieba_;
  Xapian::Database db_;
  int fd_;
};

#endif
