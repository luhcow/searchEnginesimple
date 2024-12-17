#ifndef SG_TIME_H__
#define SG_TIME_H__

#include <chrono>

class Time {
 public:
  template <typename T>
  static long long Now() {
    return std::chrono::duration_cast<T>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }
};

#endif