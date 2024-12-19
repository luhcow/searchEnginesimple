#ifndef SG_IHASH_H__
#define SG_IHASH_H__

#include <cstdint>
#include <string>

// FNV-1a 哈希函数（32位）
static uint32_t fnv1a_hash(const std::string& key) {
  const uint32_t FNV_offset_basis = 2166136261u;  // FNV-1a 32位起始值
  const uint32_t FNV_prime = 16777619u;  // FNV-1a 32位素数

  uint32_t hash = FNV_offset_basis;
  for (char c : key) {
    hash ^= static_cast<uint8_t>(c);  // 异或当前字符
    hash *= FNV_prime;                // 乘以素数
  }
  return hash;
}

// ihash 函数，将结果转为非负整数
class iHash {
 public:
  static int Hash(const std::string& key) {
    uint32_t hash = fnv1a_hash(key);
    return static_cast<int>(hash & 0x7fffffff);  // 保证结果非负
  }
};

#endif