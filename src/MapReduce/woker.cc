#include <fmt/core.h>
#include <fmt/ranges.h>
#include <wfrest/HttpServer.h>
#include <workflow/KafkaDataTypes.h>
#include <workflow/KafkaResult.h>
#include <workflow/WFFacilities.h>
#include <workflow/WFKafkaClient.h>

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "json.hpp"
#include "time.hpp"
#include "workflow/WFTaskFactory.h"
#include "workflow/Workflow.h"

const std::string url = "http://127.0.0.1:8898";

struct WokerArgs {
  int WokerId;
  std::string WokerType;
  bool WokerDo;
  std::vector<int> Hash_;
  std::string Filename;
  int ReduceId;
};

struct WokerReply {
  std::string Filename;
  std::string ConType;
  std::string Content;
  int ReduceId;
  int err;
};

static WFFacilities::WaitGroup wait_group(1);

void sig_handler(int signo) {
  wait_group.done();
}
void woker_callback(WFHttpTask *task) {
  WokerReply rep;
  const void *body;
  size_t body_len;
  task->get_resp()->get_parsed_body(&body, &body_len);

  std::string s((const char *)body, body_len);
  nlohmann::json json(s);
  rep.Filename = json["Filename"];
  rep.ConType = json["ConType"];
  rep.Content = json["Content"];
  rep.ReduceId = json["ReduceId"];
  rep.err = json["err"];
  auto series = series_of(task);
  if (rep.ConType == "map") {
    handleMapTask(&req, &rep, mapf);
  } else if (rep.ConType == "reduce") {
    handleReduceTask(&req, &rep, reducef);
  } else if (rep.ConType == "wait") {
    *series << WFTaskFactory::create_timer_task(2000, nullptr);
  } else if (rep.ConType == "done") {
    fmt::print("All tasks completed. Worker exiting.");
    return;
  } else {
    fmt::print("Unknown task type: {}\n", rep.ConType);
  }
  *series << WFTaskFactory::create_timer_task(1000, nullptr)
          << WFTaskFactory::create_http_task(
                 url + "/", 0, 2, woker_callback);
}
void Worker(
    std::function<std::pair<std::string, std::string>(
        std::string, std::string)> mapf,
    std::function<std::string(std::string, std::vector<std::string>)>
        reducef) {
  std::srand(Time::Now<std::chrono::milliseconds>());
  int id = std::rand() % 100001 + 100000;

  WokerArgs args;
  args.WokerId = id;
  args.WokerType = "map";
  args.WokerDo = false;

  WFHttpTask *task = WFTaskFactory::create_http_task(
      url + "/", 0, 2, woker_callback);
  nlohmann::json json;
  json["WokerId"] = args.WokerId;
  json["WokerId"] = args.WokerType;
  json["WokerId"] = args.WokerDo;

  json["Hash_"] = nlohmann::json::array();

  json["WokerId"] = args.Filename;
  json["WokerId"] = args.ReduceId;
  task->get_req()->append_output_body(json.dump());
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sig_handler);

  return 0;
}
