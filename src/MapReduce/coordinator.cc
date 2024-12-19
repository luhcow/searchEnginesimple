#include <fmt/core.h>
#include <fmt/ranges.h>
#include <wfrest/HttpServer.h>
#include <workflow/KafkaDataTypes.h>
#include <workflow/KafkaResult.h>
#include <workflow/WFFacilities.h>
#include <workflow/WFKafkaClient.h>

#include <chrono>
#include <csignal>
#include <ctime>
#include <mutex>
#include <string>
#include <vector>

#include "time.hpp"

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

struct MapCon {
  std::string filename;
  std::string content;
  std::vector<int> hash;
  long long status;
  bool comp;
};

struct ReduceCon {
  int id;
  long long status;
  bool comp;
};

class Coordinator {
 public:
  std::vector<MapCon> map_con_;
  bool map_do_all_;
  std::vector<ReduceCon> reduce_con_;
  bool red_do_all_;
  std::mutex mu;
  bool updateTaskStatus(std::string taskType) {
    auto all_complete = true;

    if (taskType == "map") {
      for (auto &i : map_con_) {
        ;
        if (i.status > 2 &&
            (Time::Now<std::chrono::seconds>() - i.status) > 10) {
          i.status = 0;
          all_complete = false;
        } else if (i.status == 1) {
          i.comp = true;
        }
        all_complete = all_complete && i.comp;
      }
    } else if (taskType == "reduce") {
      for (auto &i : reduce_con_) {
        if (i.status > 2 &&
            (Time::Now<std::chrono::seconds>() - i.status) > 10) {
          i.status = 0;
          all_complete = false;
        } else if (i.status == 1) {
          i.comp = true;
        }
        all_complete = all_complete && i.comp;
      }
    }
    return all_complete;
  }
  bool assignTask(std::string taskType, WokerReply &reply) {
    if (taskType == "map") {
      for (int i = 0; i < map_con_.size(); i++) {
        if (!map_con_[i].comp && (Time::Now<std::chrono::seconds>() -
                                  map_con_[i].status) > 10) {
          reply.ConType = "map";
          reply.Filename = map_con_[i].filename;
          reply.Content = map_con_[i].content;
          map_con_[i].status = Time::Now<std::chrono::seconds>();
          return true;
        }
      }

      if (map_do_all_) {
        reply.ConType = "wait";
        return true;
      }
    } else if (taskType == "reduce") {
      for (int i = 0; i < reduce_con_.size(); i++) {
        if (!reduce_con_[i].comp &&
            (Time::Now<std::chrono::seconds>() -
             reduce_con_[i].status) > 10) {
          reply.ConType = "reduce";
          reply.ReduceId = reduce_con_[i].id;
          reply.Filename = "mr-out-" + std::to_string(reply.ReduceId);
          reduce_con_[i].status = Time::Now<std::chrono::seconds>();
          return true;
        }
      }

    } else {
      reply.ConType = "done";
      return true;
    }

    return false;
  }
};

static WFFacilities::WaitGroup wait_group(1);

void sig_handler(int signo) {
  wait_group.done();
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sig_handler);

  wfrest::HttpServer svr;

  class Coordinator c;

  std::vector<std::string> files;
  const int nReduce = 10;

  c.map_con_.resize(files.size());
  c.reduce_con_.resize(nReduce);

  for (int i = 0; i < files.size(); i++) {
    c.map_con_[i].filename = files[i];
    c.map_con_[i].status = 0;
  }

  for (int i = 0; i < c.reduce_con_.size(); i++) {
    c.reduce_con_[i].id = i + 1;
    c.reduce_con_[i].comp = false;
    c.reduce_con_[i].status = 0;
  }

  svr.GET(
      "/",
      [&c](const wfrest::HttpReq *req,
           wfrest::HttpResp *resp,
           SeriesWork *series) {
        c.mu.lock();
        WokerArgs args;
        WokerReply reply;
        args.WokerId = req->json()["WokerId"].get<int>();
        args.WokerType = req->json()["WokerType"].get<std::string>();
        args.WokerDo = req->json()["WokerDo"].get<bool>();
        for (int i = 0; i < req->json()["hash"].size(); i++) {
          args.Hash_.push_back(req->json()["Hash_"][i].get<int>());
        }
        args.Filename = req->json()["Filename"].get<bool>();
        args.ReduceId = req->json()["ReduceId"].get<bool>();

        if (args.WokerDo) {
          if (args.WokerType == "map") {
            fmt::print("map task {} report done", args.Filename);
            for (int i = 0; i < c.map_con_.size(); i++) {
              if (c.map_con_[i].filename == args.Filename) {
                c.map_con_[i].hash = args.Hash_;
                c.map_con_[i].comp = true;
                c.map_con_[i].status = 1;
                fmt::print("map task {} done", args.Filename);
                break;
              }
            }

            c.map_do_all_ = c.updateTaskStatus("map");
            if (c.map_do_all_) {
              // TODO 处理零散的 map 文件 并 排序
            }
          } else if (args.WokerType == "reduce") {
            fmt::print("reduce task {} report done", args.ReduceId);
            for (int i = 0; i < c.reduce_con_.size(); i++) {
              if (c.reduce_con_[i].id == args.ReduceId) {
                c.reduce_con_[i].comp = true;
                break;
              }
            }

            c.red_do_all_ = c.updateTaskStatus("reduce");
          }
        } else {
          if (!c.map_do_all_ && c.assignTask("map", reply)) {
            fmt::print("map with {} task assigned: {}",
                       reply.ConType,
                       reply.Filename);
            reply.err = 0;
            return;
          } else if (!c.map_do_all_ &&
                     c.assignTask("reduce", reply)) {
            fmt::print("Reduce task assigned: {}", reply.ReduceId);
            reply.err = 0;
            return;
          } else {
            c.assignTask("done", reply);
            fmt::print("all task done to {}", args.WokerId);
            reply.err = 0;
            return;
          }
        }

        wfrest::Json json;
        json["Filename"] = reply.Filename;
        json["ConType"] = reply.ConType;
        json["Content"] = reply.Content;
        json["ReduceId"] = reply.ReduceId;
        json["err"] = reply.err;
        resp->Json(json);
        c.mu.unlock();
      });

  if (svr.start(8898) == 0) {
    wait_group.wait();
    svr.stop();
  } else {
    fprintf(stderr, "Cannot start server");
    exit(1);
  }

  return 0;
}
