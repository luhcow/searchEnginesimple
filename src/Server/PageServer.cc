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

#include "Page/WebPageQuery.hpp"
#include "cppcodec/base64_url.hpp"
#include "json.hpp"
#include "urlcode.hpp"

static WFFacilities::WaitGroup wait_group(1);

void sig_handler(int signo) {
  wait_group.done();
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sig_handler);

  WebPageQuery webpagequery(
      "/home/rings/searchEngine/data/newoffset.dat");

  wfrest::HttpServer svr;

  // 搜索词推荐
  svr.GET("/{sentence}",
          [&webpagequery](const wfrest::HttpReq *req,
                          wfrest::HttpResp *resp,
                          SeriesWork *series) {
            std::string sentence(req->param("sentence"));

            UrlCoder::decode(sentence);
            UrlCoder::decode(sentence);

            *series << WFTaskFactory::create_go_task(
                sentence, [&webpagequery, sentence, resp]() {
                  nlohmann::json json;
                  json["data"] = webpagequery.executeQuery(sentence);
                  resp->Json(json.dump());
                });
          });

  if (svr.track().start(9883) == 0) {
    svr.list_routes();
    wait_group.wait();
    svr.stop();
  } else {
    fprintf(stderr, "Cannot start server");
    exit(1);
  }

  return 0;
}
