#include <fstream>
#include <iostream>

// this define can avoid some logs which you don't need to care about.
#define LOGGER_LEVEL LL_WARN

#include "simhash/Simhasher.hpp"
using namespace simhash;

int main(int argc, char** argv) {
  Simhasher simhasher;
  string s(
      "日军侵占南京后，国民政府虽西迁重庆，但政府机关大部和军事统帅部"
      "却在武汉，武汉实际上成为当时全国军事、政治、经济的中心。");
  size_t topN = 5;
  uint64_t u64 = 0;
  vector<pair<string, double> > res;
  simhasher.extract(s, res, topN);
  simhasher.make(s, topN, u64);
  cout << "文本：\"" << s << "\"" << endl;
  cout << "关键词序列是: " << res << endl;
  cout << "simhash值是: " << u64 << endl;

  Simhasher content;

  std::ifstream ifs("../test/testdata/news_content.2");
  std::istreambuf_iterator<char> beg(ifs), end;
  string s2(beg, end);
  
  vector<pair<string, double> > res2;
  size_t topN2 = 1000;
  uint64_t u642 = 0;
  simhasher.extract(s2, res2, topN2);
  simhasher.make(s2, topN2, u642);
  cout << "关键词序列是: " << res2 << endl;
  cout << "simhash值是: " << u642 << endl;
  const char* bin1 = "100010110110";
  const char* bin2 = "110001110011";
  uint64_t u1, u2;
  u1 = Simhasher::binaryStringToUint64(bin1);
  u2 = Simhasher::binaryStringToUint64(bin2);

  cout << bin1 << "和" << bin2 << " simhash值的相等判断如下："
       << endl;
  cout << "海明距离阈值默认设置为3，则isEqual结果为："
       << (Simhasher::isEqual(u64, u642)) << endl;
  cout << "海明距离阈值默认设置为5，则isEqual结果为："
       << (Simhasher::isEqual(u64, u642, 5)) << endl;
  return EXIT_SUCCESS;
}