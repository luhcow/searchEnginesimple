#include <fmt/core.h>
#include <wfrest/HttpServer.h>
#include <workflow/KafkaDataTypes.h>
#include <workflow/KafkaResult.h>
#include <workflow/WFFacilities.h>
#include <workflow/WFKafkaClient.h>
#include <workflow/WFResourcePool.h>
#include <workflow/WFTaskFactory.h>

#include <csignal>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>

#include "Dictionary/Dictionary.hpp"
#include "Dictionary/KeyRecommander.hpp"
#include "cppcodec/base64_url.hpp"
#include "lrucache.hpp"
#include "urlcode.hpp"
#include "workflow/WFTask.h"
#include "workflow/Workflow.h"

static WFFacilities::WaitGroup wait_group(1);

void sig_handler(int signo) {
  wait_group.done();
}

KeyRecommander keyrecommander(
    std::bind(&dictionary::DictProducer::load,
              "/home/rings/searchEngine/data/dictIndex.dat",
              std::placeholders::_1,
              std::placeholders::_2));

std::string redis_url = "redis://127.0.0.1:16379";

int next_time = 1;

int main(int argc, char *argv[]) {
  signal(SIGINT, sig_handler);

  wfrest::HttpServer svr;

  // 搜索词推荐
  svr.GET(
      "/{key}",
      [](const wfrest::HttpReq *req,
         wfrest::HttpResp *resp,
         SeriesWork *series) {
        std::string key(req->param("key"));
        fmt::print("{}\n", key);
        UrlCoder::decode(key);
        UrlCoder::decode(key);

        auto readfunc = [=]() {
          *series << WFTaskFactory::create_go_task(key, [=]() {
            auto json = keyrecommander.execute(key);

            resp->Redis(redis_url, "SET", {key, json.dump()});
            resp->Json(json.dump());
          });
        };

        resp->Redis(
            redis_url, "GET", {key}, [=](WFRedisTask *redis_task) {
              protocol::RedisRequest *redis_req =
                  redis_task->get_req();
              protocol::RedisResponse *redis_resp =
                  redis_task->get_resp();
              int state = redis_task->get_state();

              protocol::RedisValue val;

              switch (state) {
                case WFT_STATE_SUCCESS:
                  redis_resp->get_result(val);
                  if (val.is_error()) {
                    readfunc();
                    return;
                  }
                  break;
                default:
                  readfunc();
                  return;
              }
              std::string cmd;
              std::vector<std::string> params;
              redis_req->get_command(cmd);
              redis_req->get_params(params);
              if (state == WFT_STATE_SUCCESS && cmd == "GET") {
                resp->Json(val.string_value());
              } else {
                readfunc();
                return;
              }
            });
      });

  if (svr.start(9882) == 0) {
    svr.list_routes();
    wait_group.wait();
    svr.stop();
  } else {
    fprintf(stderr, "Cannot start server");
    exit(1);
  }

  return 0;
}
