#include <signal.h>
#include <stdio.h>

#include <cstddef>

#include "Dictionary/Dictionary.hpp"
#include "Dictionary/KeyRecommander.hpp"
#include "config/config.h"
#include "srpc/rpc_types.h"
#include "stronly.srpc.h"
#include "workflow/WFFacilities.h"
#include "workflow/WFTaskFactory.h"
#include "workflow/Workflow.h"

using namespace srpc;

std::string redis_url = "redis://127.0.0.1:16379";
static WFFacilities::WaitGroup wait_group(1);
static srpc::RPCConfig config;

KeyRecommander keyrecommander(
    std::bind(&dictionary::DictProducer::load,
              "/home/rings/searchEnginesimple/data/dictIndex.dat",
              std::placeholders::_1,
              std::placeholders::_2));

void sig_handler(int signo) { wait_group.done(); }

void init() {
  if (config.load(
          "/home/rings/searchEnginesimple/src/stronly/server.conf") ==
      false) {
    perror("Load config failed");
    exit(1);
  }

  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);
}

class ServiceImpl : public stronly::Service {
 public:
  void Echo(EchoRequest *req,
            EchoResponse *resp,
            RPCContext *ctx) override {
    // 4. delete the following codes and fill your logic
    std::string key = req->message();

    WFFacilities::WaitGroup wait_read(1);

    auto readfunc = [=](SeriesWork *series) {
      *series << WFTaskFactory::create_go_task(key, [=]() {
        auto json = keyrecommander.execute(key);
        auto redis_task =
            WFTaskFactory::create_redis_task(redis_url, 2, nullptr);
        protocol::RedisRequest *redis_req = redis_task->get_req();
        redis_req->set_request("SET", {key, json.dump()});
        *series << redis_task;

        resp->set_message(json.dump());
      });
    };

    auto redis_task = WFTaskFactory::create_redis_task(
        redis_url, 2, [=](WFRedisTask *redis_task) {
          protocol::RedisRequest *redis_req = redis_task->get_req();
          protocol::RedisResponse *redis_resp =
              redis_task->get_resp();
          int state = redis_task->get_state();

          protocol::RedisValue val;

          switch (state) {
            case WFT_STATE_SUCCESS:
              redis_resp->get_result(val);
              if (val.is_error() || val.is_nil()) {
                readfunc(series_of(redis_task));
                return;
              }
              break;
            default:

              readfunc(series_of(redis_task));
              return;
          }
          std::string cmd;
          std::vector<std::string> params;
          redis_req->get_command(cmd);
          redis_req->get_params(params);
          if (state == WFT_STATE_SUCCESS && cmd == "GET") {
            resp->set_message(val.string_value());
          } else {
            readfunc(series_of(redis_task));
            return;
          }
        });

    protocol::RedisRequest *redis_req = redis_task->get_req();
    redis_req->set_request("GET", {key});
    SeriesWork *series = Workflow::create_series_work(
        redis_task,
        [&wait_read](const SeriesWork *) { wait_read.done(); });
    series->start();
    wait_read.wait();
  }
};

int main() {
  // 1. load config
  init();

  // 2. start server
  SRPCServer server;
  ServiceImpl impl;
  server.add_service(&impl);

  config.load_filter(server);

  if (server.start(config.server_port()) == 0) {
    // 3. success and wait
    fprintf(stderr,
            "stronly SRPC server started, port %u\n",
            config.server_port());
    wait_group.wait();
    server.stop();
  } else
    perror("server start");

  return 0;
}
