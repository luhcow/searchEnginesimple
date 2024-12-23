#include <stdio.h>

#include "config/config.h"
#include "srpc/rpc_types.h"
#include "stronly.srpc.h"
#include "workflow/WFFacilities.h"

using namespace srpc;

static WFFacilities::WaitGroup wait_group(1);
static srpc::RPCConfig config;

void init() {
  if (config.load(
          "/home/rings/searchEnginesimple/src/stronly/client.conf") ==
      false) {
    perror("Load config failed");
    exit(1);
  }
}

int main() {
  // 1. load config
  init();

  // 2. start client
  RPCClientParams params = RPC_CLIENT_PARAMS_DEFAULT;
  params.host = config.client_host();
  params.port = config.client_port();

  stronly::SRPCClient client(&params);
  config.load_filter(client);

  // 3. request with sync api
  EchoRequest req;
  EchoResponse resp;
  RPCSyncContext ctx;

  req.set_message("Hello");
  client.Echo(&req, &resp, &ctx);

  if (ctx.success)
    fprintf(stderr, "sync resp. %s\n", resp.message().c_str());
  else
    fprintf(stderr,
            "sync status[%d] error[%d] errmsg:%s\n",
            ctx.status_code,
            ctx.error,
            ctx.errmsg.c_str());

  // 4. request with async api

  req.set_message("Hello");

  client.Echo(&req, [](EchoResponse *resp, RPCContext *ctx) {
    if (ctx->success())
      fprintf(stderr, "async resp. %s\n", resp->message().c_str());
    else
      fprintf(stderr,
              "async status[%d] error[%d] errmsg:%s\n",
              ctx->get_status_code(),
              ctx->get_error(),
              ctx->get_errmsg());
    wait_group.done();
  });

  wait_group.wait();

  return 0;
}
