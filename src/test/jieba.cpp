#include "cppjieba/Jieba.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <regex>
#include <string>
#include <vector>

#include "ihsah.hpp"
#include "json.hpp"
#include "readAll.hpp"
#include "utf8.h"
#include "utf8/checked.h"

using namespace std;

int main(int argc, char** argv) {
  cppjieba::Jieba jieba;
  vector<string> words;
  string id("2");

  string s(std::move(ReadAll::read(
      "/home/rings/searchEnginesimple/data/The_Holy_Bible.txt")));

  // 定义正则表达式：匹配中文字符之间的换行符
  std::regex pattern("([\u4e00-\u9fa5])\n+([\u4e00-\u9fa5])");

  // 使用 std::regex_replace 进行替换
  // 将匹配到的换行符替换为空字符串
  std::string result = std::regex_replace(s, pattern, "$1$2");

  cout << "[demo] Cut With HMM" << endl;
  jieba.Cut(result, words, true);

  vector<nlohmann::json> json;
  json.resize(10);

  for (auto& i : words) {
    auto it = i.begin();
    int cp = utf8::next(it, i.end());
    if (cp <= 0x9fa5 && cp >= 0x4e00) {
      nlohmann::json jsont;
      jsont[i] = 1;
      auto t = iHash::Hash(i) % 10;
      json[t].push_back(jsont);

    } else if ((cp >= 0x0041 && cp <= 0x005a) ||
               (cp >= 0x0061 && cp <= 0x007a)) {
      for (char& j : i) {
        if (j >= 0x0041 && j <= 0x005a) {
          j = j - 0x20;
        }
      }
      nlohmann::json jsont;
      jsont[i] = 1;
      auto t = iHash::Hash(i) % 10;
      json.at(t).push_back(jsont);
    }
  }
  vector<int> zone;
  zone.resize(10);
  for (int i = 0; i < 10; i++) {
    zone[i] = ::open(("tmp-" + id + "-" + to_string(i)).c_str(),
                     O_RDWR | O_CREAT | O_TRUNC,
                     0666);
    ::write(zone[i], json[i].dump().c_str(), json[i].dump().length());
  }
  for (int i = 0; i < 10; i++) {
    ::close(zone[i]);
  }

  return EXIT_SUCCESS;
}
