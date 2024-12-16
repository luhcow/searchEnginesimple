#ifndef SG_CONFIGURATION_H__
#define SG_CONFIGURATION_H__

#include <map>
#include <string>

class Configuration {
 public:
  Configuration* getInstance() {
  }
  std::map<std::string, std::string>& getConfigMap() {
  }

 private:
  std::string config_file_path_;
  std::map<std::string, std::string> configs_;
};

#endif