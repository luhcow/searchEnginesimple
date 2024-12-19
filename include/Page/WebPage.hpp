#ifndef SG_WEBPAGE_H__
#define SG_WEBPAGE_H__

#include <unistd.h>

#include <cstdio>
#include <iostream>
#include <string>
class WebPage {
 public:
  WebPage(int id, long begin, long end)
      : id_(id), begin_(begin), end_(end) {
  }
  WebPage() = default;
  // This method lets cereal know which data members to serialize
  template <class Archive>
  void serialize(Archive& archive) {
    archive(id_,
            begin_,
            end_);  // serialize things by passing them to the archive
  }

  std::string operator()(int fd) {
    char* str = new char[end_ - begin_ + 1]();
    lseek(fd, begin_, SEEK_SET);
    int i = read(fd, str, end_ - begin_);
    std::cerr << end_ << " " << begin_ << " " << i << "\n";
    std::string ret(str, end_ - begin_);
    return ret;
  }

  int get_id() {
    return id_;
  }

 private:
  int id_;
  long begin_;
  long end_;
};

#endif