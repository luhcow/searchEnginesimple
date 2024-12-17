#ifndef SG_DICTPRODUCER_H__
#define SG_DICTPRODUCER_H__

#include <fcntl.h>
#include <unistd.h>

#include <functional>
#include <future>
#include <map>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "Dictionary/SplitTool.hpp"
#include "Page/Configuration.hpp"
#include "cppjieba/Jieba.hpp"
#include "ihsah.hpp"
#include "json.hpp"
#include "readAll.hpp"
#include "utf8.h"
#include "utf8/checked.h"

namespace dictionary {

class SplitToolCppJieba {
 public:
  static std::vector<std::vector<std::string>> map(
      cppjieba::Jieba& jieba,
      std::set<std::string>& stop,
      std::string file,
      int zone) {
    std::vector<std::string> words;
    std::vector<std::vector<std::string>> true_words;
    true_words.resize(zone);

    std::string s(std::move(ReadAll::read(file)));

    // 定义正则表达式：匹配中文字符之间的换行符
    std::regex pattern("([\u4e00-\u9fa5])\n+([\u4e00-\u9fa5])");

    // 使用 std::regex_replace 进行替换
    // 将匹配到的换行符替换为空字符串
    std::string result = std::regex_replace(s, pattern, "$1$2");
    jieba.Cut(result, words, true);

    for (auto& i : words) {
      auto it = i.begin();
      int cp = utf8::next(it, i.end());
      if (cp <= 0x9fa5 && cp >= 0x4e00) {
        if (stop.find(i) != stop.end()) {
          auto t = iHash::Hash(i) % zone;
          true_words[t].push_back(i);
        }
      } else if ((cp >= 0x0041 && cp <= 0x005a) ||
                 (cp >= 0x0061 && cp <= 0x007a)) {
        for (char& j : i) {
          if (j >= 0x0041 && j <= 0x005a) {
            j = j - 0x20;
          }
        }
        if (stop.find(i) != stop.end()) {
          auto t = iHash::Hash(i) % zone;
          true_words[t].push_back(i);
        }
      }
    }
    return true_words;
  }

  std::map<std::string, int> reduce(std::vector<std::string>& map_) {
    std::map<std::string, int> count;
    for (std::string& i : map_) {
      count[i]++;
    }
    return count;
  }
};

class DictProducer {
 public:
  DictProducer(std::string file) {
    std::string s(std::move(
        ReadAll::read("/home/rings/searchEngine/data/files.json")));
    nlohmann::json json(s);
    files_.resize(json["text"].size());
    for (int i = 0; i < files_.size(); i++) {
      files_[i] = json["text"].at(i);
    }

    for (int i = 0; i < json["stop"].size(); i++) {
      std::string t(std::move(ReadAll::read(json["stop"].at(i))));
      std::string tw;
      std::istringstream split_flow(t);
      while (split_flow >> tw) {
        stop_.insert(tw);
      }
    }
    zone_ = json["zone"];
  }
  void buildDict() {
    std::vector<std::future<std::vector<std::vector<std::string>>>>
        map_worker(files_.size());

    for (auto& i : files_) {
      map_worker.emplace_back(std::async(std::launch::async,
                                         &SplitToolCppJieba::map,
                                         std::ref(jieba_),
                                         std::ref(stop_),
                                         i,
                                         zone_));
    }

    std::vector<std::vector<std::string>> true_words;
    true_words.resize(zone_);

    for (auto& i : map_worker) {
      auto t = i.get();
      for (int j = 0; j < t.size(); j++) {
        for (auto& tt : t.at(j)) {
          true_words.at(j).push_back(tt);
        }
      }
    }
  }

  void creatIndex() {
  }
  void store() {
  }

 private:
  std::vector<std::string> files_;
  std::set<std::string> stop_;
  std::vector<std::pair<std::string, int>> dict_;
  std::map<std::string, std::set<int>> index_;
  SplitTool* cuttor_;
  cppjieba::Jieba jieba_;
  int zone_;
};

}  // namespace dictionary
#endif