#include <fmt/core.h>
#include <wfrest/HttpServer.h>
#include <workflow/KafkaDataTypes.h>
#include <workflow/KafkaResult.h>
#include <workflow/WFFacilities.h>
#include <workflow/WFKafkaClient.h>
#include <workflow/WFMessageQueue.h>
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

WFResourcePool lrucache_pool(20);

int next_time = 1;

struct lrucache_t {
  cache::lru_cache<std::string, std::string> lrucache;
  std::list<std::pair<std::string, std::string>> log;
  int max_size;
  lrucache_t(int max) : lrucache(max), max_size(max) {
  }
};

std::vector<lrucache_t> lrucache_vec;

cache::lru_cache<std::string, std::string> temp_cache(30);

void copy_pool(WFGoTask *task) {
  auto *series = series_of(task);
  lrucache_t **lrucacheptr = (lrucache_t **)series->get_context();
  auto ptr = *lrucacheptr;

  for (auto &j : (*ptr).log) {
    temp_cache.put(j.first, j.second);
  }

  fmt::print("在被覆盖之前 {} 旧的 cache size {}\n",
             (*ptr).lrucache.size());

  (*ptr).lrucache = temp_cache;
  (*ptr).log.clear();

  lrucache_pool.post(ptr);
  // for (int i = 0; i < 20; i++) {
  //   lrucache_pool.post(&lrucache_vec[i]);
  // }
}

void timer_callback(WFTimerTask *copytask) {
  next_time = next_time * 2;
  next_time = std::min(next_time, 5);

  auto series = series_of(copytask);

  for (int i = 0; i < 20; i++) {
    auto task =
        WFTaskFactory::create_go_task("falsecopy", [copytask]() {
          auto series = series_of(copytask);
          lrucache_t **lrucacheptr =
              (lrucache_t **)series->get_context();
          auto lrucache = *lrucacheptr;

          for (auto &j : lrucache->log) {
            temp_cache.put(j.first, j.second);
          }

          for (auto &i : temp_cache) {
            lrucache->lrucache.put(i.first, i.second);
          }

          lrucache->log.clear();

          lrucache_pool.post(lrucache);
        });

    void **lrucacheptrptr = (void **)series->get_context();
    WFConditional *cond =
        lrucache_pool.get(task, (void **)lrucacheptrptr);

    *series << cond;
  }

  *(series_of(copytask)) << WFTaskFactory::create_timer_task(
      next_time, 0, timer_callback);
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sig_handler);

  wfrest::HttpServer svr;

  for (int i = 0; i < 20; ++i) {
    lrucache_vec.emplace_back(30);
  }

  for (int i = 0; i < 20; i++) {
    lrucache_pool.post(&lrucache_vec[i]);
  }

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

        auto task =
            WFTaskFactory::create_go_task(key, [resp, series, key]() {
              lrucache_t **lrucacheptr =
                  (lrucache_t **)series->get_context();
              auto lrucache = *lrucacheptr;
              try {
                if (lrucache == nullptr) {
                  fmt::print("smoe thing err\n");
                  return;
                }
                const std::string &from_cache =
                    lrucache->lrucache.get(key);

                resp->Json(from_cache);
              } catch (...) {
                auto readfunc = [=]() {
                  *series << WFTaskFactory::create_go_task(
                      key, [=]() {
                        auto json = keyrecommander.execute(key);

                        lrucache->lrucache.put(key, json.dump());
                        lrucache->log.push_front({key, json.dump()});
                        if (lrucache->log.size() >
                            lrucache->max_size) {
                          lrucache->log.pop_back();
                        }

                        resp->Redis(
                            redis_url, "SET", {key, json.dump()});
                        resp->Json(json.dump());
                      });
                };

                resp->Redis(redis_url,
                            "GET",
                            {key},
                            [=](WFRedisTask *redis_task) {
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
                              if (state == WFT_STATE_SUCCESS &&
                                  cmd == "GET") {
                                if (val.is_string()) {
                                  lrucache->lrucache.put(
                                      key, val.string_value());
                                  lrucache->log.push_front(
                                      {key, val.string_value()});
                                  if (lrucache->log.size() >
                                      lrucache->max_size) {
                                    lrucache->log.pop_back();
                                  }

                                  resp->Json(val.string_value());

                                } else {
                                  readfunc();
                                  return;
                                }
                              }
                            });
              }

              lrucache_pool.post(lrucache);
            });

        cache::lru_cache<std::string, std::string> **lrucacheptrptr =
            new cache::lru_cache<std::string, std::string> *();
        series->set_context(lrucacheptrptr);

        WFConditional *cond =
            lrucache_pool.get(task, (void **)lrucacheptrptr);

        *series << cond;
        series->set_callback([lrucacheptrptr](const SeriesWork *) {
          delete lrucacheptrptr;
        });
      });

  // 聚合任务

  WFTimerTask *timer;
  timer =
      WFTaskFactory::create_timer_task(next_time, 0, timer_callback);

  SeriesWork *work =
      Workflow::create_series_work(timer, [](const SeriesWork *) {});

  cache::lru_cache<std::string, std::string> **lrucacheptrptr =
      new cache::lru_cache<std::string, std::string> *();
  work->set_context(lrucacheptrptr);

  work->start();

  if (svr.start(9883) == 0) {
    svr.list_routes();
    wait_group.wait();
    svr.stop();
  } else {
    fprintf(stderr, "Cannot start server");
    exit(1);
  }

  return 0;
}
