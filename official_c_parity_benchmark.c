#include <stdio.h>
#include <string.h>

#include <amps/amps_impl.h>

typedef struct
{
  amps_handle message;
  char frame[128];
  size_t frame_len;
  unsigned long bytes_read;
} header_parse_ctx_t;

typedef struct
{
  amps_handle message;
  char frame[96];
  size_t frame_len;
  unsigned long bytes_read;
} sow_batch_ctx_t;

typedef struct
{
  amps_handle message;
  char buffer[256];
} header_write_ctx_t;

static double now_ns(void)
{
  static LARGE_INTEGER frequency;
  static int initialized = 0;
  LARGE_INTEGER counter;

  if (!initialized)
  {
    QueryPerformanceFrequency(&frequency);
    initialized = 1;
  }

  QueryPerformanceCounter(&counter);
  return (double)counter.QuadPart * 1000000000.0 / (double)frequency.QuadPart;
}

static void bench_header_hot_parse(header_parse_ctx_t* ctx)
{
  amps_protocol_pre_deserialize(ctx->message, ctx->frame, ctx->frame_len);
  amps_protocol_deserialize(ctx->message, 0, &ctx->bytes_read);
}

static void bench_header_hot_write(header_write_ctx_t* ctx)
{
  amps_protocol_serialize(ctx->message, ctx->buffer, sizeof(ctx->buffer));
}

static void run_header_hot_write(void)
{
  header_write_ctx_t ctx;
  const long long iterations = 5000000;
  long long index;
  double start_ns;
  double end_ns;
  double ns_per_op;

  memset(&ctx, 0, sizeof(ctx));
  ctx.message = amps_message_create(NULL);
  amps_message_set_field_value_nts(ctx.message, AMPS_Command, "p");
  amps_message_set_field_value_nts(ctx.message, AMPS_CommandId, "cmd-1");
  amps_message_set_field_value_nts(ctx.message, AMPS_Topic, "orders");
  amps_message_set_field_value_nts(ctx.message, AMPS_SubscriptionId, "sub-1");
  amps_message_set_field_value_nts(ctx.message, AMPS_QueryID, "qry-1");
  amps_message_set_field_value_nts(ctx.message, AMPS_Options, "replace");
  amps_message_set_field_value_nts(ctx.message, AMPS_Filter, "/id > 10");
  amps_message_set_field_value_nts(ctx.message, AMPS_Expiration, "42");
  amps_message_set_field_value_nts(ctx.message, AMPS_Sequence, "123456789");

  for (index = 0; index < 10000; ++index)
  {
    bench_header_hot_write(&ctx);
  }

  start_ns = now_ns();
  for (index = 0; index < iterations; ++index)
  {
    bench_header_hot_write(&ctx);
  }
  end_ns = now_ns();

  ns_per_op = (end_ns - start_ns) / (double)iterations;
  printf("OfficialCParityHeaderHotWrite\t%lld\t%.2f ns/op\n", iterations, ns_per_op);

  amps_message_destroy(ctx.message);
}

static void bench_sow_batch_parse(sow_batch_ctx_t* ctx)
{
  int index;
  for (index = 0; index < 5; ++index)
  {
    amps_protocol_pre_deserialize(ctx->message, ctx->frame, ctx->frame_len);
    amps_protocol_deserialize(ctx->message, 0, &ctx->bytes_read);
  }
}

static void run_header_hot_parse(void)
{
  header_parse_ctx_t ctx;
  const long long iterations = 50000000;
  long long index;
  double start_ns;
  double end_ns;
  double ns_per_op;

  memset(&ctx, 0, sizeof(ctx));
  ctx.message = amps_message_create(NULL);
  strcpy(ctx.frame, "{\"c\":\"p\",\"t\":\"orders\",\"sub_id\":\"sub-1\"}{\"id\":1}");
  ctx.frame_len = strlen(ctx.frame);

  for (index = 0; index < 10000; ++index)
  {
    bench_header_hot_parse(&ctx);
  }

  start_ns = now_ns();
  for (index = 0; index < iterations; ++index)
  {
    bench_header_hot_parse(&ctx);
  }
  end_ns = now_ns();

  ns_per_op = (end_ns - start_ns) / (double)iterations;
  printf("OfficialCParityHeaderHotParse\t%lld\t%.2f ns/op\n", iterations, ns_per_op);

  amps_message_destroy(ctx.message);
}

static void run_sow_batch_parse(void)
{
  sow_batch_ctx_t ctx;
  const long long iterations = 15000000;
  long long index;
  double start_ns;
  double end_ns;
  double ns_per_outer;

  memset(&ctx, 0, sizeof(ctx));
  ctx.message = amps_message_create(NULL);
  strcpy(ctx.frame, "{\"c\":\"p\",\"t\":\"orders\",\"sub_id\":\"sub-1\"}{\"id\":1}");
  ctx.frame_len = strlen(ctx.frame);

  for (index = 0; index < 5000; ++index)
  {
    bench_sow_batch_parse(&ctx);
  }

  start_ns = now_ns();
  for (index = 0; index < iterations; ++index)
  {
    bench_sow_batch_parse(&ctx);
  }
  end_ns = now_ns();

  ns_per_outer = (end_ns - start_ns) / (double)iterations;
  printf("OfficialCParitySOWBatchParse\t%lld\t%.2f ns/op\n", iterations, ns_per_outer);

  amps_message_destroy(ctx.message);
}

int main(void)
{
  printf("Official AMPS C parser parity benchmarks\n");
  run_header_hot_write();
  run_header_hot_parse();
  run_sow_batch_parse();
  return 0;
}
