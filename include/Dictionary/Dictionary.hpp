#ifndef SG_DICTPRODUCER_H__
#define SG_DICTPRODUCER_H__

#include <dirent.h>
#include <fcntl.h>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cereal/archives/binary.hpp>
#include <cereal/types/map.hpp>
#include <cereal/types/set.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "cppjieba/Jieba.hpp"
#include "ihsah.hpp"
#include "json.hpp"
#include "readAll.hpp"
#include "utf8.h"
#include "utf8/core.h"
#include "utf8/cpp11.h"

namespace dictionary {

class DirScanner {
 public:
  std::vector<std::string> operator()(std::string dir) {
    traverse(dir);
    return files_;
  }
  void traverse(std::string path) {
    // 打开目录
    DIR* path_dir = opendir(path.c_str());
    struct dirent* entry;
    // 遍历目录流，依次删除每一个目录项
    while ((entry = readdir(path_dir)) != NULL) {
      if (entry->d_name[0] == '.') {
        continue;
      }
      if (entry->d_type == DT_DIR) {
        char new_src[255];
        sprintf(new_src, "%s/%s", path.c_str(), entry->d_name);
        traverse(new_src);
      }
      if (entry->d_type != DT_DIR) {
        char new_src[255];
        sprintf(new_src, "%s/%s", path.c_str(), entry->d_name);
        files_.push_back(new_src);
      }
    }

    // 关闭目录流
    closedir(path_dir);
    // 目录为空了，可以删除该目录了
    // 目录为空了，可以删除该目录了
  }

 private:
  std::vector<std::string> files_;
};

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
        if (stop.find(i) == stop.end()) {
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
        if (stop.find(i) == stop.end()) {
          auto t = iHash::Hash(i) % zone;
          true_words[t].push_back(i);
        }
      }
    }
    return true_words;
  }

  static std::map<std::string, int> reduce(
      std::vector<std::string>& map_) {
    std::map<std::string, int> count;
    for (std::string& i : map_) {
      count[i]++;
    }
    return count;
  }
};

class IndexTool {
 public:
  static void map(std::string& word,
                  int index,
                  std::map<int, std::set<int>>& index_map) {
    std::map<int, std::set<int>>& ret = index_map;
    utf8::iterator<std::string::iterator> beg(
        word.begin(), word.begin(), word.end());
    utf8::iterator<std::string::iterator> end(
        word.end(), word.begin(), word.end());
    for (; beg != end; beg++) {
      ret[*beg].insert(index);
    }
  }
};
class DictProducer {
 public:
  DictProducer(std::string file) {
    std::string s(ReadAll::read(file));
    nlohmann::json json = nlohmann::json::parse(s);

    for (int i = 0; i < json["text"].size(); i++) {
      auto j = DirScanner()(json["text"].at(i));
      for (auto& k : j) {
        files_.push_back(k);
      }
    }

    for (int i = 0; i < json["stop"].size(); i++) {
      std::string t(ReadAll::read(json["stop"].at(i)));
      std::string tw;
      std::istringstream split_flow(t);
      while (split_flow >> tw) {
        stop_.insert(tw);
      }
    }
    zone_ = json["zone"];
    dat_file_ = json["data"];
    // load();
  }
  void buildDict() {
    std::vector<std::future<std::vector<std::vector<std::string>>>>
        map_worker;

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

    std::vector<std::future<std::map<std::string, int>>>
        reduce_worker;

    for (auto& i : true_words) {
      std::sort(i.begin(), i.end());
      reduce_worker.emplace_back(
          std::async(std::launch::async,
                     &SplitToolCppJieba::reduce,
                     std::ref(i)));
    }

    std::map<std::string, int> true_map;
    for (auto& i : reduce_worker) {
      auto t = i.get();
      true_map.insert(t.begin(), t.end());
    }

    for (auto it = true_map.begin(); it != true_map.end(); it++) {
      dict_.push_back(*it);
    }

    fmt::print("fict_ is {}", dict_);
  }

  void creatIndex() {
    for (int i = 0; i < dict_.size(); i++) {
      IndexTool::map(dict_[i].first, i, index_);
    }
  }
  void store() {
    std::ofstream os(dat_file_, std::ios::binary);
    cereal::BinaryOutputArchive archive(os);
    archive(dict_, index_);
  }

  static void load(std::string dat_file,
                   std::vector<std::pair<std::string, int>>& dict,
                   std::map<int, std::set<int>>& index) {
    std::ifstream is(dat_file, std::ios::binary);
    cereal::BinaryInputArchive archive(is);
    archive(dict, index);
  }

 private:
  std::vector<std::string> files_;
  std::set<std::string> stop_;
  std::vector<std::pair<std::string, int>> dict_;
  std::map<int, std::set<int>> index_;
  cppjieba::Jieba jieba_;
  int zone_;
  std::string dat_file_;
};

}  // namespace dictionary
#endif