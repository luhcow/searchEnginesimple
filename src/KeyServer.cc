#include <fmt/core.h>
#include <wfrest/HttpServer.h>
#include <workflow/KafkaDataTypes.h>
#include <workflow/KafkaResult.h>
#include <workflow/WFFacilities.h>
#include <workflow/WFKafkaClient.h>
#include <workflow/WFTaskFactory.h>

#include <csignal>
#include <functional>
#include <iostream>
#include <string>

#include "Dictionary/Dictionary.hpp"
#include "KeyRecommander.hpp"
#include "cppcodec/base64_url.hpp"
#include "urlcode.hpp"

static WFFacilities::WaitGroup wait_group(1);

void sig_handler(int signo) {
  wait_group.done();
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sig_handler);

  KeyRecommander keyrecommander(
      std::bind(&dictionary::DictProducer::load,
                "/home/rings/searchEngine/data/dictIndex.dat",
                std::placeholders::_1,
                std::placeholders::_2));

  wfrest::HttpServer svr;

  // 搜索词推荐
  svr.GET("/{key}",
          [&keyrecommander](const wfrest::HttpReq *req,
                            wfrest::HttpResp *resp,
                            SeriesWork *series) {
            std::string key(req->param("key"));
            fmt::print("{}\n", key);
            UrlCoder::decode(key);
            UrlCoder::decode(key);

            *series << WFTaskFactory::create_go_task(
                key, [&keyrecommander, key, resp]() {
                  auto json = keyrecommander.execute(key);
                  resp->Json(json.dump());
                });
          });

  if (svr.track().start(9882) == 0) {
    svr.list_routes();
    wait_group.wait();
    svr.stop();
  } else {
    fprintf(stderr, "Cannot start server");
    exit(1);
  }

  return 0;
}
