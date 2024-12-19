#ifndef SG_WEBPAGEQUERY_H__
#define SG_WEBPAGEQUERY_H__

#include <fcntl.h>
#include <fmt/core.h>
#include <fmt/ranges.h>

#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "Page/WebPage.hpp"
#include "cereal/archives/json.hpp"
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
  WebPageQuery(std::string bina) {
    std::ifstream is(bina, std::ios::binary);
    cereal::JSONInputArchive archive(is);

    archive(page_list_, true_map_, invert_index_lib_, page_index_);
    // fmt::print("page-list is {}", page_list_);
    fd_ = open("/home/rings/searchEngine/data/newripepage.dat",
               O_RDONLY);
  }

  nlohmann::json executeQuery(std::string sentence) {
    std::vector<std::string> words;
    jieba_.Cut(sentence, words, true);
    auto weight = getQueryWordsWeightVector(words);

    fmt::print("weight before is {}\n", weight);

    std::map<std::string, std::map<int, double>> query;

    for (auto& key : weight) {
      query[key.first].insert(invert_index_lib_[key.first].begin(),
                              invert_index_lib_[key.first].end());
    }

    for (auto it = query.begin(); it != query.end();) {
      if (it->second.size() < 1) {
        it = query.erase(it);  // erase 返回指向下一个有效元素的迭代器
      } else {
        ++it;  // 如果不删除，则手动递增迭代器
      }
    }

    fmt::print("\nquery is {}\n", query);
    // 获取网页交集
    std::set<int> get_page;
    auto fir = query.begin();
    if (fir == query.end()) {
      nlohmann::json json;
      json = nullptr;
      return json;
    }
    for (auto& j : (*(fir)).second) {
      get_page.insert(j.first);
    }
    fmt::print("\nget_page before is {}\n", get_page);
    for (auto& i : query) {
      std::vector<int> ready_delete;

      for (auto& j : get_page) {
        if (i.second.count(j) == 0) {
          ready_delete.push_back(j);
        }
      }
      for (auto j : ready_delete) {
        get_page.erase(j);
      }
    }
    fmt::print("\nget_page is {}\n", get_page);
    // 计算余弦相似度
    std::map<int, double> candidate;

    for (auto& i : get_page) {
      double xy = 0, X2 = 0, Y2 = 0;
      for (auto& j : words) {
        auto y = invert_index_lib_[j][i];
        xy += weight[j] * y;
        X2 += pow(weight[j], 2);
        Y2 += pow(y, 2);
      }
      double cos = xy / (sqrt(X2) * sqrt(Y2));
      candidate[i] = cos;
    }

    // 优先级队列找前十个
    std::function<bool(std::pair<int, double>&,
                       std::pair<int, double>&)>
        com = [](std::pair<int, double>& lhs,
                 std::pair<int, double>& rhs) -> bool {
      return lhs.second > rhs.second;
    };

    std::priority_queue<std::pair<int, double>,
                        std::vector<std::pair<int, double>>,
                        std::function<bool(std::pair<int, double>&,
                                           std::pair<int, double>&)>>
        que(com);

    for (auto& i : candidate) {
      que.push(i);
    }

    nlohmann::json json;

    for (int i = 0; i < std::min(10, (int)que.size()); i++) {
      nlohmann::json page =
          nlohmann::json::parse(page_list_[que.top().first](fd_));
      json[i]["title"] = page["title"];
      json[i]["link"] = page["link"];

      // std::string tem(
      //     page["content"].get<std::string>().begin(),
      //     page["content"].get<std::string>().begin() +
      //         std::min(
      //             40,
      //             (int)page["content"].get<std::string>().length()));

      json[i]["content"] = page["content"];
      que.pop();
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
  int fd_;
};

#endif
