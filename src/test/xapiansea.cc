#include <xapian.h>

#include <iostream>
#include <string>

#include "cppjieba/Jieba.hpp"

int main() {
  cppjieba::Jieba jieba;
  // 打开 Xapian 数据库（只读模式）
  std::string db_path = "/home/rings/searchEnginesimple/data/page_db";
  Xapian::Database db(db_path);

  // 用户输入的查询关键词
  std::string user_query = "人工智能";
  // 对查询关键词进行分词
  std::vector<std::string> query_words;
  jieba.Cut(user_query, query_words);

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
  Xapian::Enquire enquire(db);
  enquire.set_query(query);

  // 获取查询结果（返回前 10 个结果）
  Xapian::MSet matches = enquire.get_mset(0, 10);

  // 输出检索结果
  std::cout << "检索结果: " << matches.size() << " 个匹配项"
            << std::endl;
  for (Xapian::MSetIterator it = matches.begin(); it != matches.end();
       ++it) {
    Xapian::Document doc = it.get_document();

    std::cout << "- 匹配得分: " << it.get_weight() << std::endl;

    std::cout << "  内容: \n"
              << doc.get_data().substr(doc.get_data().find("\n") + 1)
              << std::endl;
    std::cout << "  link: " << doc.get_value(1) << std::endl;
  }

  return 0;
}
