#include <fcntl.h>
#include <unistd.h>

#include <iostream>
#include <regex>
#include <string>

#include "json.hpp"
#include "simhash/Simhasher.hpp"
#include "tinyxml2.h"

std::string extractTextFromPTags(const std::string& htmlContent) {
  // 正则表达式去匹配 <p> 标签中的内容，并忽略属性和标签
  std::regex pTagRegex("<p.*?>(.*?)</p>", std::regex::icase);
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

struct RssItem {
  std::string title;
  std::string link;
  std::string description;
  std::string content;
};

std::vector<RssItem> kRss;

void create_xml(const char* Path) {
  simhash::Simhasher simhasher;
  size_t topN = 5;
  uint64_t u64 = 0;

  int j = 0;
  nlohmann::json json;
  json["data"] = nlohmann::json::array();
  for (auto i : kRss) {
    nlohmann::json page;
    page["docid"] = j++;
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
  int f = open(Path, O_RDWR | O_CREAT | O_TRUNC, 0666);
  write(f, json.dump(2).c_str(), json.dump(2).length());
}

int main() {
  tinyxml2::XMLDocument doc;
  doc.LoadFile(
      "/home/rings/searchEngine/data/人民网语料/finance.xml");

  tinyxml2::XMLElement* findElement = doc.FirstChildElement("rss");
  findElement = findElement->FirstChildElement("channel");
  // findElement = findElement->FirstChildElement("title");
  // const char* title = findElement->GetText();
  // printf("Name of play (2): %s\n", title);

  for (tinyxml2::XMLElement* currentele =
           findElement->FirstChildElement("item");
       currentele;
       currentele = currentele->NextSiblingElement()) {
    RssItem rss;

    tinyxml2::XMLElement* titleElement =
        currentele->FirstChildElement("title");

    std::string title(titleElement->GetText());
    rss.title = title;
    std::cout << title << "\n";

    tinyxml2::XMLElement* linkElement =
        currentele->FirstChildElement("link");
    std::string link(linkElement->GetText());
    rss.link = link;

    std::cout << link << "\n";

    tinyxml2::XMLElement* descriptionElement =
        currentele->FirstChildElement("description");
    std::string description(descriptionElement->GetText());
    std::string description_ex = extractTextFromPTags(description);
    rss.description = description_ex;
    std::cout << description_ex << "\n";
    // const char* description = descriptionElement->GetText();
    // printf("Name of play (1): %s\n", description);

    tinyxml2::XMLElement* content_encodedElement =
        currentele->FirstChildElement("content");
    if (content_encodedElement == nullptr) {
      content_encodedElement =
          currentele->FirstChildElement("description");
    }
    std::string content_encoded(content_encodedElement->GetText());
    std::string content_encoded_ex =
        extractTextFromPTags(content_encoded);
    rss.content = content_encoded_ex;
    std::cout << content_encoded_ex << "\n";
    // const char* content_encoded =
    // content_encodedElement->GetText();
    //  printf("Name of play (1): %s\n", content_encoded);
    kRss.push_back(rss);
  }
  create_xml("pagelib.txt");
}