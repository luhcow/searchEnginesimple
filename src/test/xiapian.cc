#include <xapian.h>

#include <iostream>
#include <string>
#include <vector>

#include "cppjieba/Jieba.hpp"
#include "json.hpp"

int main() {
  cppjieba::Jieba jieba;
  // 创建或打开 Xapian 数据库（路径：./example_db）
  std::string db_path = "/home/rings/searchEngine/data/page_db";
  Xapian::WritableDatabase db(db_path, Xapian::DB_CREATE_OR_OPEN);

  // 定义文章数据
  struct Document {
    std::string id;
    std::string title;
    std::string content;
    std::string link;
  };

  std::vector<Document> documents;

  // 打开文件
  std::ifstream file("/home/rings/searchEngine/data/output.json");

  nlohmann::json json;
  file >> json;

  for (int i = 0; i < json.size(); i++) {
    documents.push_back({std::to_string(i),
                         json[i]["title"].get<std::string>(),
                         json[i]["content"].get<std::string>(),
                         json[i]["link"].get<std::string>()});
  }

  // 遍历文章并添加到数据库
  for (const auto& doc_data : documents) {
    // 创建一个 Xapian 文档对象
    Xapian::Document doc;

    // 设置文章内容
    std::string data = doc_data.title + "\n" + doc_data.content;
    doc.set_data(data);

    // 创建 TermGenerator，用于为文档生成索引
    Xapian::TermGenerator term_generator;
    term_generator.set_document(doc);
    // 使用 cppjieba 对标题和内容分词
    std::vector<std::string> title_words;
    jieba.Cut(doc_data.title, title_words);

    std::vector<std::string> content_words;
    jieba.Cut(doc_data.content, content_words);

    // 将分词结果转为以空格分隔的字符串
    std::string title_tokens = "";
    for (const auto& word : title_words) {
      title_tokens += word + " ";
    }

    std::string content_tokens = "";
    for (const auto& word : content_words) {
      content_tokens += word + " ";
    }

    // 为标题和内容生成索引
    term_generator.index_text(title_tokens, 1, "S");  // 标题前缀 "S"
    term_generator.index_text(content_tokens);  // 内容无前缀
    // 添加唯一标识符（如 ID）作为布尔值项
    doc.add_boolean_term("Q" + doc_data.id);
    doc.add_value(1, doc_data.link);

    // 将文档添加到数据库
    db.add_document(doc);
  }

  std::cout << "文章已成功添加到 Xapian 数据库！" << std::endl;

  return 0;
}
