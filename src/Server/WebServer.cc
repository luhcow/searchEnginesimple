#include <csignal>
#include <string>

#include "cppcodec/base64_url.hpp"
#include "urlcode.hpp"
#include "wfrest/HttpServer.h"
#include "workflow/KafkaDataTypes.h"
#include "workflow/KafkaResult.h"
#include "workflow/WFFacilities.h"
#include "workflow/WFKafkaClient.h"
using namespace wfrest;

static WFFacilities::WaitGroup wait_group(1);

bool no_cgroup = false;

WFKafkaClient client;

void kafka_callback(WFKafkaTask *task) {
  std::cerr << "callback in\n";
  int state = task->get_state();
  int error = task->get_error();

  if (state != WFT_STATE_SUCCESS) {
    fprintf(stderr,
            "error msg: %s\n",
            WFGlobal::get_error_string(state, error));
    fprintf(stderr, "Failed. Press Ctrl-C to exit.\n");
    client.deinit();
    wait_group.done();
    return;
  }

  WFKafkaTask *next_task = NULL;
  std::vector<std::vector<protocol::KafkaRecord *>> records;
  std::vector<protocol::KafkaToppar *> toppars;
  int api_type = task->get_api_type();

  protocol::KafkaResult new_result;

  std::cerr << api_type << "\n";
  switch (api_type) {
    case Kafka_Produce:
      task->get_result()->fetch_records(records);

      for (const auto &v : records) {
        for (const auto &w : v) {
          const void *value;
          size_t value_len;
          w->get_value(&value, &value_len);
          printf(
              "produce\ttopic: %s, partition: %d, status: %d, \
						offset: %lld, val_len: %zu\n",
              w->get_topic(),
              w->get_partition(),
              w->get_status(),
              w->get_offset(),
              value_len);
        }
      }

      break;

    case Kafka_Fetch:
      new_result = std::move(*task->get_result());
      new_result.fetch_records(records);

      if (!records.empty()) {
        std::cerr << "no empty\n";
        if (!no_cgroup)
          next_task = client.create_kafka_task(
              "api=commit", 3, kafka_callback);

        std::string out;

        for (const auto &v : records) {
          if (v.empty())
            continue;

          char fn[1024];
          snprintf(fn,
                   1024,
                   "kafka.%s.%d.%llu",
                   v.back()->get_topic(),
                   v.back()->get_partition(),
                   v.back()->get_offset());

          FILE *fp = fopen(fn, "w+");
          std::cerr << fn << "\n";
          long long offset = 0;
          int partition = 0;
          std::string topic;

          for (const auto &w : v) {
            const void *value;
            size_t value_len;
            w->get_value(&value, &value_len);
            if (fp)
              fwrite(value, value_len, 1, fp);
            std::cerr << value;
            offset = w->get_offset();
            partition = w->get_partition();
            topic = w->get_topic();

            if (!no_cgroup)
              next_task->add_commit_record(*w);
          }

          if (!topic.empty()) {
            out += "topic: " + topic;
            out += ",partition: " + std::to_string(partition);
            out += ",offset: " + std::to_string(offset) + ";";
          }

          if (fp)
            fclose(fp);
        }

        printf("fetch\t%s\n", out.c_str());

        if (!no_cgroup)
          series_of(task)->push_back(next_task);
      }

      break;

    case Kafka_OffsetCommit:
      task->get_result()->fetch_toppars(toppars);

      if (!toppars.empty()) {
        for (const auto &v : toppars) {
          printf(
              "commit\ttopic: %s, partition: %d, \
						offset: %llu, error: %d\n",
              v->get_topic(),
              v->get_partition(),
              v->get_offset(),
              v->get_error());
        }
      }

      next_task = client.create_leavegroup_task(3, kafka_callback);

      series_of(task)->push_back(next_task);

      break;

    case Kafka_LeaveGroup:
      printf("leavegroup callback\n");
      break;

    default:
      break;
  }

  if (!next_task) {
    client.deinit();
    wait_group.done();
  }
}

void sig_handler(int signo) {
  wait_group.done();
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sig_handler);

  // example:
  // kafka://10.160.23.23:9000,10.123.23.23,kafka://kafka.sogou
  std::string kafkaurl = "kafka://localhost:9092";
  std::string sugesturl = "http://localhost:9882/";
  std::string weburl = "http://localhost:9883/";

  if (client.init(kafkaurl) < 0) {
    perror("client.init");
    exit(1);
  }

  HttpServer svr;

  // 搜索词推荐
  svr.GET("/sug/{key}",
          [&sugesturl](const HttpReq *req, HttpResp *resp) {
            std::string key(req->param("key"));

            UrlCoder::decode(key);
            UrlCoder::decode(key);
            resp->Http(sugesturl + key);
          });

  // 网页搜索
  svr.GET(
      "/s/{key}",
      [&weburl](
          const HttpReq *req, HttpResp *resp, SeriesWork *series) {
        std::string key(req->param("key"));

        UrlCoder::decode(key);
        UrlCoder::decode(key);
        resp->Http(weburl + key);
      });

  // 把跳转的信息送给 kafka
  svr.GET(
      "/link/{url}",
      [](const HttpReq *req, HttpResp *resp, SeriesWork *series) {
        auto dec = cppcodec::base64_url::decode(req->param("url"));
        std::string url(dec.begin(), dec.end());
        Json jump_info = Json::parse(url);

        auto task = client.create_kafka_task(
            "api=produce", 3, kafka_callback);
        protocol::KafkaConfig config;
        protocol::KafkaRecord record;

        config.set_compress_type(Kafka_NoCompress);
        config.set_client_id("webSearch");
        task->set_config(std::move(config));

        record.set_key(jump_info["url"].get<std::string>().c_str(),
                       jump_info["url"].get<std::string>().length());

        record.set_value(jump_info["data"].dump().c_str(),
                         jump_info["data"].dump().length());

        // record.add_header_pair("hk2", 3, "hv2", 3);

        task->add_produce_record(
            "SearchResultsJump", -1, std::move(record));

        series->push_back(task);

        resp->set_status(302);
        resp->add_header("Location",
                         jump_info["url"].get<std::string>());
      });

  if (svr.track().start(8888) == 0) {
    wait_group.wait();
    svr.stop();
  } else {
    fprintf(stderr, "Cannot start server");
    exit(1);
  }

  return 0;
}
