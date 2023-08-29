#include <arpa/inet.h>
#include <assert.h>
#include <endian.h>
#include <errno.h>
#include <limits.h>
#include <netdb.h>
#include <poll.h>
#include <rdma/rdma_cma.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h> /* for struct timeval */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <fcntl.h>

#define MIN(a, b) (a) < (b) ? a : b
#define RDMA_MAX_SGE 128
#define RDMA_CLIENT_TX_SIZE 1024
#define RDMA_CLIENT_RX_SIZE 8192

#define REDIS_ERR -1
#define REDIS_OK 0

#define REDIS_ERR_IO 1       /* Error in read or write */
#define REDIS_ERR_EOF 3      /* End of file */
#define REDIS_ERR_PROTOCOL 4 /* Protocol error */
#define REDIS_ERR_OOM 5      /* Out of memory */
#define REDIS_ERR_TIMEOUT 6  /* Timed out */
#define REDIS_ERR_OTHER 2    /* Everything else... */

#define REDIS_CONNECTED 0x2

typedef struct redisContext
{
  int err;          /* Error flags, 0 when there is no error */
  char errstr[128]; /* String representation of error when applicable */

  // a single flag indicating the connection status
  int flags;

  char *obuf;       /* Write buffer */
  size_t obuf_size; /* Size of write buffer */

  struct
  {
    char *host;
    char *source_addr;
    int port;
  } tcp;

  void *privctx;

} redisContext;

void redisFree(redisContext *c)
{
  if (c == NULL)
    return;

  free(c->obuf);
  free(c->tcp.host);
  free(c->tcp.source_addr);

  memset(c, 0xff, sizeof(*c));
  free(c);
}

typedef struct RdmaContext
{
  struct rdma_cm_id *cm_id;
  struct rdma_event_channel *cm_channel;
  struct ibv_cq *send_cq;
  struct ibv_cq *recv_cq;
  struct ibv_pd *pd;

  /* TX */
  char *send_buf;
  uint32_t send_length;
  struct ibv_mr *send_mr;

  bool *send_status;
  struct ibv_mr *status_mr;

  /* RX */
  char *recv_buf;
  uint32_t recv_length;
  uint32_t recv_offset;
  uint32_t outstanding_msg_size;
  struct ibv_mr *recv_mr;

} RdmaContext;

static inline long redisNowMs(void)
{
  struct timeval tv;

  if (gettimeofday(&tv, NULL) < 0)
    return -1;

  return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

void __redisSetError(redisContext *c, int type, const char *str)
{
  size_t len;

  c->err = type;
  if (str != NULL)
  {
    len = strlen(str);
    len = len < (sizeof(c->errstr) - 1) ? len : (sizeof(c->errstr) - 1);
    memcpy(c->errstr, str, len);
    c->errstr[len] = '\0';
  }
  else
  {
    /* Only REDIS_ERR_IO may lack a description! */
    // assert(type == REDIS_ERR_IO);
    strerror_r(errno, c->errstr, sizeof(c->errstr));
  }
  printf("error: %s\n", c->errstr);
}

static size_t rdmaPostSend(RdmaContext *ctx, struct rdma_cm_id *cm_id,
                           const void *data, size_t data_len)
{
  struct ibv_send_wr send_wr, *bad_wr;
  struct ibv_sge sge;
  uint32_t index;
  uint64_t addr;

  /* find an unused send buffer entry */
  for (index = 0; index < RDMA_MAX_SGE; index++)
  {
    if (!ctx->send_status[index])
    {
      break;
    }
  }

  // not likely to fill up the send buffer
  assert(index < RDMA_MAX_SGE);
  assert(data_len <= RDMA_CLIENT_TX_SIZE);

  ctx->send_status[index] = true;
  addr = ctx->send_buf + index * RDMA_CLIENT_TX_SIZE;
  memcpy(addr, data, data_len);

  sge.addr = (uint64_t)addr;
  sge.lkey = ctx->send_mr->lkey;
  sge.length = data_len;

  send_wr.sg_list = &sge;
  send_wr.num_sge = 1;
  send_wr.wr_id = index;
  send_wr.opcode = IBV_WR_SEND;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.next = NULL;
  printf("rdmaPostSend: %d with index %d\n", data_len, index);
  if (ibv_post_send(cm_id->qp, &send_wr, &bad_wr))
  {
    return -1;
  }

  return data_len;
}

static int rdmaPostRecv(RdmaContext *ctx, struct rdma_cm_id *cm_id,
                        uint32_t index)
{
  struct ibv_sge sge;
  struct ibv_recv_wr recv_wr, *bad_wr;

  sge.addr = (uint64_t)(ctx->recv_buf + index * RDMA_CLIENT_RX_SIZE);
  sge.length = RDMA_CLIENT_RX_SIZE;
  sge.lkey = ctx->recv_mr->lkey;

  recv_wr.wr_id = index;
  recv_wr.sg_list = &sge;
  recv_wr.num_sge = 1;
  recv_wr.next = NULL;

  // printf("rdmaPostRecv: index=%d\n", index);
  if (ibv_post_recv(cm_id->qp, &recv_wr, &bad_wr))
  {
    return -1;
  }

  return 0;
}

static void rdmaDestroyIoBuf(RdmaContext *ctx)
{
  if (ctx->recv_mr)
  {
    ibv_dereg_mr(ctx->recv_mr);
    ctx->recv_mr = NULL;
  }

  munmap(ctx->recv_buf, RDMA_MAX_SGE * RDMA_CLIENT_RX_SIZE);
  ctx->recv_buf = NULL;

  if (ctx->send_mr)
  {
    ibv_dereg_mr(ctx->send_mr);
    ctx->send_mr = NULL;
  }

  munmap(ctx->send_buf, RDMA_MAX_SGE * RDMA_CLIENT_TX_SIZE);
  ctx->send_buf = NULL;

  if (ctx->status_mr)
  {
    ibv_dereg_mr(ctx->status_mr);
    ctx->status_mr = NULL;
  }

  munmap(ctx->send_status, RDMA_MAX_SGE * sizeof(bool));
  ctx->send_status = NULL;
}

static int rdmaSetupIoBuf(redisContext *c, RdmaContext *ctx,
                          struct rdma_cm_id *cm_id)
{
  int access = IBV_ACCESS_LOCAL_WRITE;
  size_t length = RDMA_MAX_SGE * sizeof(bool);
  ctx->send_status = mmap(NULL, length, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ctx->status_mr = ibv_reg_mr(ctx->pd, ctx->send_status, length, access);
  if (!ctx->status_mr)
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: reg status mr failed");
    goto destroy_iobuf;
  }

  // note: ban remote read and write
  //   access =
  //       IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  length = RDMA_MAX_SGE * RDMA_CLIENT_RX_SIZE;
  ctx->recv_buf = mmap(NULL, length, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ctx->recv_length = length;
  ctx->recv_offset = 0;
  ctx->outstanding_msg_size = 0;
  ctx->recv_mr = ibv_reg_mr(ctx->pd, ctx->recv_buf, length, access);
  if (!ctx->recv_mr)
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: reg recv mr failed");
    goto destroy_iobuf;
  }

  length = RDMA_MAX_SGE * RDMA_CLIENT_TX_SIZE;
  ctx->send_buf = mmap(NULL, length, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ctx->send_length = length;
  ctx->send_mr = ibv_reg_mr(ctx->pd, ctx->send_buf, length, access);
  if (!ctx->send_mr)
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: reg send buf mr failed");
    goto destroy_iobuf;
  }

  return 0;

destroy_iobuf:
  rdmaDestroyIoBuf(ctx);
  return -1;
}

static int connRdmaHandleSend(RdmaContext *ctx, uint32_t index)
{
  ctx->send_status[index] = false;

  return 0;
}

static int connRdmaHandleRecv(redisContext *c, RdmaContext *ctx,
                              struct rdma_cm_id *cm_id, uint32_t index,
                              uint32_t byte_len)
{
  assert(byte_len > 0);
  assert(byte_len <= RDMA_CLIENT_RX_SIZE);
  ctx->recv_offset = index * RDMA_CLIENT_RX_SIZE;
  ctx->outstanding_msg_size = byte_len;

  // todo: directly read from the recv buffer

  // to replenish the recv buffer
  return rdmaPostRecv(ctx, cm_id, index);
}

int connRdmaHandleCq(redisContext *c, bool rx)
{
  RdmaContext *ctx = (RdmaContext *)c->privctx;
  struct rdma_cm_id *cm_id = ctx->cm_id;
  struct ibv_wc wc = {0};
  int ret;

pollcq:
  if (rx)
  {
    ret = ibv_poll_cq(ctx->recv_cq, 1, &wc);
    // printf("poll recv cq\n");
  }
  else
  {
    ret = ibv_poll_cq(ctx->send_cq, 1, &wc);
    // printf("poll send cq\n");
  }
  if (ret < 0)
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: poll cq failed");
    return -1;
  }
  else if (ret == 0)
  {
    return 0;
  }

  if (wc.status != IBV_WC_SUCCESS)
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: send/recv failed");
    return -1;
  }

  switch (wc.opcode)
  {
  case IBV_WC_RECV:
    printf("received messages\n");
    if (connRdmaHandleRecv(c, ctx, cm_id, wc.wr_id, wc.byte_len) == -1)
    {
      return -1;
    }
    printf("received %d bytes, id %d\n", wc.byte_len, wc.wr_id);

    // todo: magic number
    return 0xff;

    break;

  case IBV_WC_SEND:
    printf("sent messages\n");
    if (connRdmaHandleSend(ctx, wc.wr_id) == -1)
    {
      return -1;
    }
    break;
  default:
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: unexpected opcode");
    return -1;
  }

  goto pollcq;

  return 0;
}

static ssize_t redisRdmaRead(redisContext *c, char *buf, size_t bufcap)
{
  RdmaContext *ctx = (RdmaContext *)c->privctx;
  // struct rdma_cm_id *cm_id = ctx->cm_id;
  uint32_t toread;

  if (ctx->outstanding_msg_size > 0)
  {
    toread = MIN(ctx->outstanding_msg_size, bufcap);
    memcpy(buf, ctx->recv_buf + ctx->recv_offset, toread);

    ctx->recv_offset = 0;
    ctx->outstanding_msg_size = 0;

    return toread;
  }
}

static ssize_t redisRdmaWrite(redisContext *c)
{
  printf("sending messages\n");
  RdmaContext *ctx = (RdmaContext *)c->privctx;
  struct rdma_cm_id *cm_id = ctx->cm_id;

  if (rdmaPostSend(ctx, cm_id, c->obuf, c->obuf_size) == (size_t)REDIS_ERR)
  {
    return REDIS_ERR;
  }

  return c->obuf_size;
}

static void redisRdmaClose(redisContext *c)
{
  printf("closing connection\n");
  RdmaContext *ctx = (RdmaContext *)c->privctx;
  struct rdma_cm_id *cm_id = ctx->cm_id;

  connRdmaHandleCq(c, true);
  connRdmaHandleCq(c, false);
  rdma_disconnect(cm_id);
  ibv_destroy_cq(ctx->send_cq);
  ibv_destroy_cq(ctx->recv_cq);
  rdmaDestroyIoBuf(ctx);
  ibv_destroy_qp(cm_id->qp);
  ibv_dealloc_pd(ctx->pd);
  rdma_destroy_id(cm_id);

  rdma_destroy_event_channel(ctx->cm_channel);
}

static int redisRdmaConnect(redisContext *c, struct rdma_cm_id *cm_id)
{
  printf("route resolved, creating RDMA structs\n");
  RdmaContext *ctx = (RdmaContext *)c->privctx;
  struct ibv_cq *send_cq, *recv_cq;
  struct ibv_pd *pd = NULL;
  struct ibv_qp_init_attr init_attr = {0};
  struct rdma_conn_param conn_param = {0};

  pd = ibv_alloc_pd(cm_id->verbs);
  if (!pd)
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: alloc pd failed");
    goto error;
  }

  send_cq = ibv_create_cq(cm_id->verbs, RDMA_MAX_SGE, NULL, NULL, 0);
  if (!send_cq)
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: create send cq failed");
    goto error;
  }

  recv_cq = ibv_create_cq(cm_id->verbs, RDMA_MAX_SGE, NULL, NULL, 0);
  if (!recv_cq)
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: create recv cq failed");
    goto error;
  }

  /* create qp with attr */
  init_attr.cap.max_send_wr = RDMA_MAX_SGE;
  init_attr.cap.max_recv_wr = RDMA_MAX_SGE;
  init_attr.cap.max_send_sge = 1;
  init_attr.cap.max_recv_sge = 1;
  init_attr.qp_type = IBV_QPT_RC;
  init_attr.send_cq = send_cq;
  init_attr.recv_cq = recv_cq;
  init_attr.srq = NULL;
  init_attr.sq_sig_all = 1;
  if (rdma_create_qp(cm_id, pd, &init_attr))
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: create qp failed");
    goto error;
  }

  ctx->cm_id = cm_id;
  ctx->send_cq = send_cq;
  ctx->recv_cq = recv_cq;
  ctx->pd = pd;

  if (rdmaSetupIoBuf(c, ctx, cm_id) != REDIS_OK)
    goto free_qp;

  /* rdma connect with param */
  conn_param.responder_resources = 0;
  conn_param.initiator_depth = 0;
  conn_param.qp_num = ctx->cm_id->qp->qp_num;
  printf("RDMA: connecting to %s:%d\n", c->tcp.host, c->tcp.port);
  if (rdma_connect(cm_id, &conn_param))
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: connect failed");
    goto destroy_iobuf;
  }

  return REDIS_OK;

destroy_iobuf:
  rdmaDestroyIoBuf(ctx);
free_qp:
  ibv_destroy_qp(cm_id->qp);
error:
  if (send_cq)
    ibv_destroy_cq(send_cq);
  if (recv_cq)
    ibv_destroy_cq(recv_cq);
  if (pd)
    ibv_dealloc_pd(pd);

  return REDIS_ERR;
}

static int redisRdmaEstablished(redisContext *c, struct rdma_cm_id *cm_id)
{
  RdmaContext *ctx = (RdmaContext *)c->privctx;

  /* it's time to tell redis we have already connected */
  c->flags |= REDIS_CONNECTED;
  // c->funcs = &redisContextRdmaFuncs;
  // todo, will a Null fd crash the program?
  // c->fd = -1;

  printf("connection established\n");

  // start to accept data
  for (int i = 0; i < RDMA_MAX_SGE; i++)
  {
    if (rdmaPostRecv(ctx, cm_id, i) == REDIS_ERR)
    {
      __redisSetError(c, REDIS_ERR_OTHER, "RDMA: post recv failed");
      // goto destroy_iobuf;
    }
  }

  return REDIS_OK;
}

static int redisRdmaCM(redisContext *c, int timeout)
{
  RdmaContext *ctx = (RdmaContext *)c->privctx;
  struct rdma_cm_event *event;
  char errorstr[128];
  int ret = REDIS_ERR;

  while (rdma_get_cm_event(ctx->cm_channel, &event) == 0)
  {
    printf("event: %s\n", rdma_event_str(event->event));
    switch (event->event)
    {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
      if (timeout < 0 || timeout > 100)
        timeout = 100; /* at most 100ms to resolve route */
      ret = rdma_resolve_route(event->id, timeout);
      if (ret)
      {
        __redisSetError(c, REDIS_ERR_OTHER, "RDMA: route resolve failed");
      }
      break;
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
      ret = redisRdmaConnect(c, event->id);
      break;
    case RDMA_CM_EVENT_ESTABLISHED:
      ret = redisRdmaEstablished(c, event->id);
      break;
    case RDMA_CM_EVENT_TIMEWAIT_EXIT:
      ret = REDIS_ERR;
      __redisSetError(c, REDIS_ERR_TIMEOUT, "RDMA: connect timeout");
      break;
    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_UNREACHABLE:
    case RDMA_CM_EVENT_REJECTED:
    case RDMA_CM_EVENT_DISCONNECTED:
    case RDMA_CM_EVENT_ADDR_CHANGE:
    default:
      snprintf(errorstr, sizeof(errorstr), "RDMA: connect failed - %s",
               rdma_event_str(event->event));
      __redisSetError(c, REDIS_ERR_OTHER, errorstr);
      ret = REDIS_ERR;
      break;
    }

    rdma_ack_cm_event(event);
  }

  return ret;
}

static int redisRdmaWaitConn(redisContext *c, long timeout)
{
  printf("waiting connection from server side\n");
  int timed;
  struct pollfd pfd;
  long now = redisNowMs();
  long start = now;
  RdmaContext *ctx = (RdmaContext *)c->privctx;

  while (now - start < timeout)
  {
    timed = (int)(timeout - (now - start));

    pfd.fd = ctx->cm_channel->fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    if (poll(&pfd, 1, timed) < 0)
    {
      return REDIS_ERR;
    }

    if (redisRdmaCM(c, timed) == REDIS_ERR)
    {
      return REDIS_ERR;
    }

    if (c->flags & REDIS_CONNECTED)
    {
      return REDIS_OK;
    }

    now = redisNowMs();
  }

  return REDIS_ERR;
}

int redisContextConnectRdma(redisContext *c, const char *addr, int port,
                            const struct timeval *timeout)
{
  printf("initiate connection between client and servers\n");
  int ret;
  char _port[6]; /* strlen("65535"); */
  struct addrinfo hints, *servinfo, *p;
  long timeout_msec = -1;
  struct rdma_event_channel *cm_channel = NULL;
  struct rdma_cm_id *cm_id = NULL;
  RdmaContext *ctx = NULL;
  struct sockaddr_storage saddr;
  long start = redisNowMs(), timed;

  servinfo = NULL;
  // c->connection_type = REDIS_CONN_RDMA;
  c->tcp.port = port;

  if (c->tcp.host != addr)
  {
    free(c->tcp.host);

    c->tcp.host = strdup(addr);
    if (c->tcp.host == NULL)
    {
      __redisSetError(c, REDIS_ERR_OOM, "RDMA: Out of memory");
      return REDIS_ERR;
    }
  }

  snprintf(_port, 6, "%d", port);
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  if ((ret = getaddrinfo(c->tcp.host, _port, &hints, &servinfo)) != 0)
  {
    hints.ai_family = AF_INET6;
    if ((ret = getaddrinfo(addr, _port, &hints, &servinfo)) != 0)
    {
      __redisSetError(c, REDIS_ERR_OTHER, gai_strerror(ret));
      return REDIS_ERR;
    }
  }

  // ctx = hi_calloc(sizeof(RdmaContext), 1);
  ctx = calloc(1, sizeof(RdmaContext));
  if (!ctx)
  {
    __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
    goto error;
  }

  c->privctx = ctx;

  cm_channel = rdma_create_event_channel();
  if (!cm_channel)
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: create event channel failed");
    goto error;
  }

  ctx->cm_channel = cm_channel;

  if (rdma_create_id(cm_channel, &cm_id, (void *)ctx, RDMA_PS_TCP))
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: create id failed");
    return REDIS_ERR;
  }
  ctx->cm_id = cm_id;

  // note, let's make it blocking for now
  // if ((redisSetFdBlocking(c, cm_channel->fd, 0) != REDIS_OK))
  // {
  //   __redisSetError(c, REDIS_ERR_OTHER,
  //                   "RDMA: set cm channel fd non-block failed");
  //   goto free_rdma;
  // }

  // make it non-blocking
  int flags = fcntl(cm_channel->fd, F_GETFL, 0);
  fcntl(cm_channel->fd, F_SETFL, flags | O_NONBLOCK);

  for (p = servinfo; p != NULL; p = p->ai_next)
  {
    if (p->ai_family == PF_INET)
    {
      memcpy(&saddr, p->ai_addr, sizeof(struct sockaddr_in));
      ((struct sockaddr_in *)&saddr)->sin_port = htons(port);
    }
    else if (p->ai_family == PF_INET6)
    {
      memcpy(&saddr, p->ai_addr, sizeof(struct sockaddr_in6));
      ((struct sockaddr_in6 *)&saddr)->sin6_port = htons(port);
    }
    else
    {
      __redisSetError(c, REDIS_ERR_PROTOCOL, "RDMA: unsupported family");
      goto free_rdma;
    }

    /* resolve addr as most 100ms */
    printf("rdma resolving addr\n");
    if (rdma_resolve_addr(cm_id, NULL, (struct sockaddr *)&saddr, 100))
    {
      continue;
    }

    // note: no timeout specified, negative number means large enough
    timed = timeout_msec - (redisNowMs() - start);
    if (redisRdmaWaitConn(c, 50) == REDIS_OK) //&& (c->flags & REDIS_CONNECTED))
    {
      ret = REDIS_OK;
      printf("rdma connect success\n");
      goto end;
    }
  }

  if ((!c->err) && (p == NULL))
  {
    __redisSetError(c, REDIS_ERR_OTHER, "RDMA: resolve failed");
  }

free_rdma:
  if (cm_id)
  {
    rdma_destroy_id(cm_id);
  }
  if (cm_channel)
  {
    rdma_destroy_event_channel(cm_channel);
  }

error:
  ret = REDIS_ERR;
  if (ctx)
  {
    free(ctx);
  }

end:
  if (servinfo)
  {
    freeaddrinfo(servinfo);
  }

  return ret;
}

static void redisRdmaFree(void *privctx)
{
  if (!privctx)
    return;

  free(privctx);
}

int main(int argc, char **argv)
{

  if (argc != 3)
  {
    printf("usage: ./client <ip> <port>\n");
    return -1;
  }

  const char *ip = argv[1];
  int port = atoi(argv[2]);

  redisContext *c = calloc(1, sizeof(redisContext));
  if (!c)
  {
    printf("failed to allocate memory\n");
    return -1;
  }

  char msg[] = "hello world";
  c->obuf = msg;
  c->obuf_size = sizeof(msg);

  c->tcp.host = strdup(ip);
  c->tcp.port = port;

  // redisContextConnectRdma -> redisRdmaWaitConn -> redisRdmaCM
  redisContextConnectRdma(c, ip, port, NULL);
  printf("start writing\n");
  // write and read staff
  redisRdmaWrite(c);

  printf("end writing\n");
  while (true)
  {
    connRdmaHandleCq(c, false);
    if (connRdmaHandleCq(c, true) == 0xff)
    {
      // connRdmaHandleRecv(c, (RdmaContext *)c->privctx, ((RdmaContext *)c->privctx)->cm_id, 0, 0);
      char msg_in[1024];
      redisRdmaRead(c, msg_in, sizeof(msg_in));

      printf("received: %s\n", msg_in);
    }
  }

  // close connection
  redisRdmaClose(c);
}
