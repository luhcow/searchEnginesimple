#ifndef SG_PAGE_H__
#define SG_PAGE_H__

#include <dirent.h>
#include <fcntl.h>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <unistd.h>

#include <cmath>
#include <cstdint>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <regex>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Page/WebPage.hpp"
#include "cereal/archives/binary.hpp"
#include "cereal/types/map.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/vector.hpp"
#include "cppjieba/Jieba.hpp"
#include "ihsah.hpp"
#include "json.hpp"
#include "readAll.hpp"
#include "simhash/Simhasher.hpp"
#include "tinyxml2.h"
#include "utf8.h"
namespace Page {

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
  static std::map<std::string, int> reduce(
      std::vector<std::string>& map_) {
    std::map<std::string, int> count;
    for (std::string& i : map_) {
      count[i]++;
    }
    return count;
  }
};

class PageTools {
 public:
  std::string map(std::string file) {
    clean(file);
    std::string Path(file.begin(),
                     file.begin() + file.find_last_of("."));
    Path += ".json";
    simhash::Simhasher simhasher;
    size_t topN = 5;
    uint64_t u64 = 0;

    int j = 0;
    nlohmann::json json;
    json["data"] = nlohmann::json::array();
    for (auto i : kRss) {
      nlohmann::json page;
      page["docid"] = j;
      page["title"] = i.title;
      page["link"] = i.link;
      page["description"] = i.description;
      page["content"] = i.content;
      if (i.content.length() >= 10) {
        simhasher.make(i.content, topN, u64);
      } else {
        simhasher.make(i.description, topN, u64);
      }
      page["simhash"] = std::to_string(u64);
      json["data"].push_back(page);
    }
    int f = open(Path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    write(f, json.dump(2).c_str(), json.dump(2).length());
    close(f);
    return Path;
  }

  void reduce(std::string file, long& num, int fd) {
    fmt::print("get filename {}\n", file);
    nlohmann::json json;
    json = nlohmann::json::parse(ReadAll::read(file));
    for (nlohmann::json j : json["data"]) {
      fmt::print("\n\nread json some j {} \n\n", j.dump());
      unsigned long simhasht =
          std::stoul(j["simhash"].get<std::string>());
      if (simhasht == 0 || hashs_.find(simhasht) != hashs_.end()) {
        fmt::print("\n\nsimhash repeat is {} \n\n", simhasht);
        continue;
      }
      hashs_.insert(simhasht);

      nlohmann::json k;

      if (j["content"].get<std::string>().length() >= 10) {
        k["content"] = j["content"];
        fmt::print("\nget content {}\n", k.dump());
      } else if (j["description"].get<std::string>().length() >= 10) {
        k["content"] = j["description"];
        fmt::print("\nget description {}\n", k.dump());
      } else {
        fmt::print("\nget nothing {}\n", k.dump());
        continue;
      }

      k["title"] = j["title"];
      k["link"] = j["link"];
      k["simhash"] = j["simhash"];
      write(fd, "          ", 10);
      write(fd, k.dump().c_str(), k.dump().length());
      write(fd, "          ", 10);
      std::string po(k.dump().c_str(), k.dump().length());
      fmt::print("\nthis json will write {}\n", po);
      pages_.emplace_back(
          pages_.size(), num + 5, num + k.dump().length() + 10 + 5);

      num += (k.dump().length() + 20);
    }
  }

  std::vector<WebPage> pages_;

 private:
  std::string extractTextFromPTags(const std::string& htmlContent) {
    // 正则表达式去匹配 <p> 标签中的内容，并忽略属性和标签
    std::regex pTagRegex("<p>(.*?)</p>", std::regex::icase);
    std::regex removeTagRegex("<[^>]*>");
    std::regex removeOtherRegex("&nbsp");
    std::string result;

    auto wordsBegin = std::sregex_iterator(
        htmlContent.begin(), htmlContent.end(), pTagRegex);
    auto wordsEnd = std::sregex_iterator();

    for (std::sregex_iterator i = wordsBegin; i != wordsEnd; ++i) {
      std::smatch match = *i;
      std::string pText = match.str(1);
      // 去除标签和属性
      pText = std::regex_replace(pText, removeTagRegex, "");
      pText = std::regex_replace(pText, removeOtherRegex, "");
      result += pText;
    }

    return result;
  }

  void clean(std::string file) {
    tinyxml2::XMLDocument doc;
    doc.LoadFile(file.c_str());

    tinyxml2::XMLElement* findElement = doc.FirstChildElement("rss");
    findElement = findElement->FirstChildElement("channel");

    for (tinyxml2::XMLElement* currentele =
             findElement->FirstChildElement("item");
         currentele;
         currentele = currentele->NextSiblingElement()) {
      RssItem rss;

      tinyxml2::XMLElement* titleElement =
          currentele->FirstChildElement("title");

      std::string title(titleElement->GetText());
      rss.title = title;

      tinyxml2::XMLElement* linkElement =
          currentele->FirstChildElement("link");
      std::string link(linkElement->GetText());
      rss.link = link;

      tinyxml2::XMLElement* descriptionElement =
          currentele->FirstChildElement("description");
      if (descriptionElement != nullptr) {
        std::string description(descriptionElement->GetText());
        std::string description_ex =
            extractTextFromPTags(description);
        rss.description = description_ex;
      } else {
        rss.description = "";
      }
      tinyxml2::XMLElement* content_encodedElement =
          currentele->FirstChildElement("content");
      if (content_encodedElement != nullptr) {
        std::string content_encoded(
            content_encodedElement->GetText());
        std::string content_encoded_ex =
            extractTextFromPTags(content_encoded);
        rss.content = content_encoded_ex;
      } else {
        rss.content = "";
      }

      kRss.push_back(rss);
    }
  }
  struct RssItem {
    std::string title;
    std::string link;
    std::string description;
    std::string content;
  };

  std::vector<RssItem> kRss;

  std::set<unsigned long,
           std::function<bool(unsigned long lhs, unsigned long rhs)>>
      hashs_{
          std::function<bool(unsigned long lhs, unsigned long rhs)>(
              [](unsigned long lhs, unsigned long rhs) -> bool {
                if (simhash::Simhasher::isEqual(lhs, rhs)) {
                  return false;
                } else {
                  return lhs < rhs;
                }
              })};
};

class PageLibPreprocessor {
 public:
  void cutRedundantPage() {
    for (auto& i : files_) {
      i = PageTools().map(i);
    }
    PageTools reducer;

    long num = 0;
    for (auto& i : files_) {
      reducer.reduce(i, num, fd_);
    }
    page_list_ = reducer.pages_;
  }

  void buildDict() {
    for (int t = 0; t < page_list_.size(); t++) {
      std::vector<std::string> words;

      fmt::print("\nmaybe rad something {}\n", page_list_[t](fd_));
      std::string s(page_list_[t](fd_));
      fmt::print("\nmaybe s {}\n", s);
      // 定义正则表达式：匹配中文字符之间的换行符
      std::regex pattern("([\u4e00-\u9fa5])\n+([\u4e00-\u9fa5])");

      // 使用 std::regex_replace 进行替换
      // 将匹配到的换行符替换为空字符串
      std::string result = std::regex_replace(s, pattern, "$1$2");
      jieba_.Cut(result, words, true);

      for (auto& i : words) {
        auto it = i.begin();
        int cp = utf8::next(it, i.end());
        if (cp <= 0x9fa5 && cp >= 0x4e00) {
          if (stop_.find(i) == stop_.end()) {
            page_index_[t][i]++;
            invert_index_lib_[i][t] = 0.0;
          }
        } else if ((cp >= 0x0041 && cp <= 0x005a) ||
                   (cp >= 0x0061 && cp <= 0x007a)) {
          for (char& j : i) {
            if (j >= 0x0041 && j <= 0x005a) {
              j = j - 0x20;
            }
          }
          if (stop_.find(i) == stop_.end()) {
            page_index_[t][i]++;
            invert_index_lib_[i][t] = 0.0;
          }
        }
      }
    }

    // 权重
    for (auto& key : invert_index_lib_) {
      for (auto& pag : key.second) {
        auto TF = page_index_[pag.first][key.first];
        auto DF = invert_index_lib_[key.first].size();
        double IDF = log2(double(page_list_.size()) / (DF + 1));
        pag.second = IDF * TF;
      }
    }

    // 归一化
    for (auto& pag : page_index_) {
      double sqr = 0.0;
      for (auto& wd : pag.second) {
        sqr += pow(invert_index_lib_[wd.first][pag.first], 2);
      }
      sqr = sqrt(sqr);
      for (auto& wd : pag.second) {
        invert_index_lib_[wd.first][pag.first] /= sqr;
      }
    }
    fmt::print("\nreduce is done {}\n", true_map_);
  }

  void buildLnvertIndexMap() {
    std::vector<std::string> true_words;
    for (int i = 0; i < page_list_.size(); i++) {
      std::vector<std::string> words;

      fmt::print("\nmaybe rad something {}\n", page_list_[i](fd_));
      std::string s(page_list_[i](fd_));
      fmt::print("\nmaybe s {}\n", s);
      // 定义正则表达式：匹配中文字符之间的换行符
      std::regex pattern("([\u4e00-\u9fa5])\n+([\u4e00-\u9fa5])");

      // 使用 std::regex_replace 进行替换
      // 将匹配到的换行符替换为空字符串
      std::string result = std::regex_replace(s, pattern, "$1$2");
      jieba_.Cut(result, words, true);

      for (auto& i : words) {
        auto it = i.begin();
        int cp = utf8::next(it, i.end());
        if (cp <= 0x9fa5 && cp >= 0x4e00) {
          if (stop_.find(i) == stop_.end()) {
            true_words.push_back(i);
          }
        } else if ((cp >= 0x0041 && cp <= 0x005a) ||
                   (cp >= 0x0061 && cp <= 0x007a)) {
          for (char& j : i) {
            if (j >= 0x0041 && j <= 0x005a) {
              j = j - 0x20;
            }
          }
          if (stop_.find(i) == stop_.end()) {
            true_words.push_back(i);
          }
        }
      }
    }
    fmt::print("\nmap is done {}\n", true_words);
  }

  void storeOnDisk() {
    std::ofstream os("/home/rings/searchEngine/data/newoffset.dat",
                     std::ios::binary);
    cereal::BinaryOutputArchive archive(os);
    archive(page_list_, true_map_, invert_index_lib_, page_index_);
  }

  PageLibPreprocessor(std::vector<std::string> path) {
    for (auto& i : path) {
      auto j = DirScanner()(i);
      for (auto& k : j) {
        files_.push_back(k);
      }
    }

    std::string s(
        ReadAll::read("/home/rings/searchEngine/conf/files.json"));
    nlohmann::json json = nlohmann::json::parse(s);

    for (int i = 0; i < json["stop"].size(); i++) {
      std::string t(ReadAll::read(json["stop"].at(i)));
      std::string tw;
      std::istringstream split_flow(t);
      while (split_flow >> tw) {
        stop_.insert(tw);
      }
    }
    zone_ = 10;

    fd_ = open("/home/rings/searchEngine/data/newripepage.dat",
               O_RDWR | O_CREAT | O_TRUNC,
               0666);
  }

  void close_fd() {
    close(fd_);
  }

 private:
  std::vector<std::string> files_;
  std::vector<WebPage> page_list_;
  std::unordered_map<int, std::pair<int, int>> offset_lib_;
  std::map<std::string, std::map<int, double>> invert_index_lib_;
  std::map<int, std::map<std::string, int>> page_index_;
  std::set<std::string> stop_;
  cppjieba::Jieba jieba_;
  int zone_;
  std::map<std::string, int> true_map_;
  int fd_;
};

}  // namespace Page

#endif