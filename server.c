#include <assert.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <signal.h>
#include <stdbool.h>

#include <fcntl.h>
#include <poll.h>

#define ANET_ERR_LEN 256
#define CONFIG_BINDADDR_MAX 16

#define ANET_OK 0
#define ANET_ERR -1
#define ANET_ERR_LEN 256

#define C_OK 0
#define C_ERR -1
#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3

#define MIN(a, b) (a) < (b) ? a : b
#define REDIS_MAX_SGE 128
#define REDIS_RDMA_SERVER_RX_SIZE 1024
#define REDIS_RDMA_SERVER_TX_SIZE 8192

#define NET_IP_STR_LEN 46 /* INET6_ADDRSTRLEN is 46, but we need to be sure */
#define MAX_ACCEPTS_PER_CALL 1000

typedef struct rdma_connection
{
    struct rdma_cm_id *cm_id;
    int last_errno;
} rdma_connection;

struct redisServer
{
    char neterr[ANET_ERR_LEN];           /* Error buffer for anet.c */
    char *bindaddr[CONFIG_BINDADDR_MAX]; /* Addresses we should bind to */
    int bindaddr_count;                  /* Number of addresses in server.bindaddr[] */
    long long proto_max_bulk_len;        /* Protocol bulk length maximum size. */
    int port;

    bool connected;
    // socketFds rdmafd
    rdma_connection *rdma_conn;
};

static void serverNetError(char *err, const char *fmt, ...)
{
    va_list ap;

    if (!err)
        return;
    va_start(ap, fmt);
    vsnprintf(err, ANET_ERR_LEN, fmt, ap);
    va_end(ap);

    vprintf(fmt, ap);
};

// Function to get the string representation of the log level
const char *logLevelString(int level)
{
    switch (level)
    {
    case LL_VERBOSE:
        return "VERBOSE";
    case LL_DEBUG:
        return "DEBUG";
    case LL_WARNING:
        return "WARNING";
    default:
        return "UNKNOWN";
    }
}

void serverLog(int level, const char *fmt, ...)
{
    // For this simple example, we're just printing the log to the standard output.
    // In a real-world scenario, you might write to a log file or use a logging library.

    // Get the current time for timestamping the log entry
    time_t now;
    time(&now);
    char buf[sizeof "YYYY-MM-DD HH:MM:SS"];
    strftime(buf, sizeof buf, "%Y-%m-%d %H:%M:%S", localtime(&now));

    // Print log level and timestamp
    printf("[%s] [%s] ", buf, logLevelString(level));

    // Process the format string with variable arguments
    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);

    // Print newline to finish the log entry
    printf("\n");
}

typedef struct RdmaContext
{
    char *ip;
    int port;
    struct ibv_pd *pd;
    struct rdma_event_channel *cm_channel;
    struct ibv_cq *send_cq;
    struct ibv_cq *recv_cq;
    // long long timeEvent;

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

/* Global vars */
struct redisServer server; /* Server global state */
static struct rdma_event_channel *listen_channel;
static struct rdma_cm_id *listen_cmids[CONFIG_BINDADDR_MAX];

static size_t rdmaPostSend(RdmaContext *ctx, struct rdma_cm_id *cm_id, const void *data, size_t data_len)
{
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    uint32_t index;
    uint64_t addr;

    /* find an unused send buffer entry */
    for (index = 0; index < REDIS_MAX_SGE; index++)
    {
        if (!ctx->send_status[index])
        {
            break;
        }
    }

    // not likely to fill up the send buffer
    assert(index < REDIS_MAX_SGE);
    assert(data_len <= REDIS_RDMA_SERVER_TX_SIZE);

    ctx->send_status[index] = true;
    addr = ctx->send_buf + index * REDIS_RDMA_SERVER_TX_SIZE;
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
    serverLog(LL_DEBUG, "post send, length: %d", data_len);
    if (ibv_post_send(cm_id->qp, &send_wr, &bad_wr))
    {
        return C_ERR;
    }

    return data_len;
}

static int rdmaPostRecv(RdmaContext *ctx, struct rdma_cm_id *cm_id, uint32_t index)
{
    struct ibv_sge sge;
    struct ibv_recv_wr recv_wr, *bad_wr;

    sge.addr = (uint64_t)(ctx->recv_buf + index * REDIS_RDMA_SERVER_RX_SIZE);
    sge.length = REDIS_RDMA_SERVER_RX_SIZE;
    sge.lkey = ctx->recv_mr->lkey;

    recv_wr.wr_id = index;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;
    recv_wr.next = NULL;

    if (ibv_post_recv(cm_id->qp, &recv_wr, &bad_wr))
    {
        return C_ERR;
    }
    serverLog(LL_WARNING, "rdmaPostRecv: index=%d", index);

    return C_OK;
}

static void rdmaDestroyIoBuf(RdmaContext *ctx)
{
    if (ctx->recv_mr)
    {
        ibv_dereg_mr(ctx->recv_mr);
        ctx->recv_mr = NULL;
    }

    munmap(ctx->recv_buf, REDIS_MAX_SGE * REDIS_RDMA_SERVER_RX_SIZE);
    ctx->recv_buf = NULL;

    if (ctx->send_mr)
    {
        ibv_dereg_mr(ctx->send_mr);
        ctx->send_mr = NULL;
    }

    munmap(ctx->send_buf, REDIS_MAX_SGE * REDIS_RDMA_SERVER_TX_SIZE);
    ctx->send_buf = NULL;

    if (ctx->status_mr)
    {
        ibv_dereg_mr(ctx->status_mr);
        ctx->status_mr = NULL;
    }

    munmap(ctx->send_status, REDIS_MAX_SGE * sizeof(bool));
    ctx->send_status = NULL;
}

static int rdmaSetupIoBuf(RdmaContext *ctx, struct rdma_cm_id *cm_id)
{
    int access = IBV_ACCESS_LOCAL_WRITE;
    size_t length = REDIS_MAX_SGE * sizeof(bool);
    ctx->send_status = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ctx->status_mr = ibv_reg_mr(ctx->pd, ctx->send_status, length, access);
    if (!ctx->status_mr)
    {
        serverLog(LL_WARNING, "RDMA: reg mr for status failed");
        goto destroy_iobuf;
    }

    // note: only grant local write access
    // access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    length = REDIS_MAX_SGE * REDIS_RDMA_SERVER_RX_SIZE;
    ctx->recv_buf = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ctx->recv_length = length;
    ctx->recv_offset = 0;
    ctx->outstanding_msg_size = 0;
    ctx->recv_mr = ibv_reg_mr(ctx->pd, ctx->recv_buf, length, access);
    if (!ctx->recv_mr)
    {
        serverLog(LL_WARNING, "RDMA: reg mr for recv buffer failed");
        goto destroy_iobuf;
    }

    length = REDIS_MAX_SGE * REDIS_RDMA_SERVER_TX_SIZE;
    ctx->send_buf = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ctx->send_length = length;
    ctx->send_mr = ibv_reg_mr(ctx->pd, ctx->send_buf, length, access);
    if (!ctx->send_mr)
    {
        serverLog(LL_WARNING, "RDMA: reg mr for send buffer failed");
        goto destroy_iobuf;
    }

    return C_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(ctx);
    return C_ERR;
}

static int rdmaHandleEstablished(struct rdma_cm_event *ev)
{
    server.connected = true;
    return C_OK;
}

static int rdmaHandleDisconnect(struct rdma_cm_event *ev)
{
    // skip it for now
    return C_OK;
}

static int connRdmaHandleSend(RdmaContext *ctx, uint32_t index)
{
    ctx->send_status[index] = false;
    return C_OK;
}

static int connRdmaWrite(rdma_connection *rdma_conn, const void *data, size_t data_len)
{
    // rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;

    return rdmaPostSend(ctx, cm_id, data, data_len);
}

static int connRdmaHandleRecv(RdmaContext *ctx, struct rdma_cm_id *cm_id, uint32_t index, uint32_t byte_len)
{
    assert(byte_len > 0);
    assert(byte_len <= REDIS_RDMA_SERVER_RX_SIZE);
    ctx->recv_offset = index * REDIS_RDMA_SERVER_RX_SIZE;
    ctx->outstanding_msg_size = byte_len;

    // just print this info and reply to the client
    printf("received message: %s\n", ctx->recv_buf + ctx->recv_offset);
    char *msg = "hello from server";
    connRdmaWrite(server.rdma_conn, msg, strlen(msg) + 1);

    // to replenish the recv buffer
    return rdmaPostRecv(ctx, cm_id, index);
}

static int connRdmaHandleCq(rdma_connection *rdma_conn, bool rx)
{
    // serverLog(LL_VERBOSE, "RDMA: cq handle\n");
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    struct ibv_wc wc = {0};
    int ret;

pollcq:
    if (rx)
    {
        ret = ibv_poll_cq(ctx->recv_cq, 1, &wc);
        // printf("poll rx cq\n");
    }
    else
    {
        ret = ibv_poll_cq(ctx->send_cq, 1, &wc);
        // printf("poll tx cq\n");
    }
    if (ret < 0)
    {
        serverLog(LL_WARNING, "RDMA: poll recv CQ error");
        return C_ERR;
    }
    else if (ret == 0)
    {
        return C_OK;
    }

    serverLog(LL_VERBOSE, "RDMA: received opcode 0x[%x]", wc.opcode);

    if (wc.status != IBV_WC_SUCCESS)
    {
        serverLog(LL_WARNING, "RDMA: CQ handle error status 0x%x", wc.status);
        return C_ERR;
    }

    switch (wc.opcode)
    {
    case IBV_WC_RECV:
        if (connRdmaHandleRecv(ctx, cm_id, wc.wr_id, wc.byte_len) == C_ERR)
        {
            return C_ERR;
        }
        break;

    case IBV_WC_SEND:
        if (connRdmaHandleSend(ctx, wc.wr_id) == C_ERR)
        {
            return C_ERR;
        }

        break;

    default:
        serverLog(LL_WARNING, "RDMA: unexpected opcode 0x[%x]", wc.opcode);
        return C_ERR;
    }

    goto pollcq;
}

static int rdmaCreateResource(RdmaContext *ctx, struct rdma_cm_id *cm_id)
{
    int ret = C_OK;
    struct ibv_qp_init_attr init_attr;
    struct ibv_cq *send_cq = NULL;
    struct ibv_cq *recv_cq = NULL;
    struct ibv_pd *pd = NULL;

    pd = ibv_alloc_pd(cm_id->verbs);
    if (!pd)
    {
        serverLog(LL_WARNING, "RDMA: ibv alloc pd failed");
        return C_ERR;
    }

    ctx->pd = pd;

    send_cq = ibv_create_cq(cm_id->verbs, REDIS_MAX_SGE, NULL, NULL, 0);
    if (!send_cq)
    {
        serverLog(LL_WARNING, "RDMA: ibv create send cq failed");
        return C_ERR;
    }

    recv_cq = ibv_create_cq(cm_id->verbs, REDIS_MAX_SGE, NULL, NULL, 0);
    if (!recv_cq)
    {
        serverLog(LL_WARNING, "RDMA: ibv create recv cq failed");
        return C_ERR;
    }

    ctx->send_cq = send_cq;
    ctx->recv_cq = recv_cq;

    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = REDIS_MAX_SGE;
    init_attr.cap.max_recv_wr = REDIS_MAX_SGE;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = send_cq;
    init_attr.recv_cq = recv_cq;
    init_attr.srq = NULL;
    ret = rdma_create_qp(cm_id, pd, &init_attr);
    if (ret)
    {
        serverLog(LL_WARNING, "RDMA: create qp failed");
        return C_ERR;
    }

    if (rdmaSetupIoBuf(ctx, cm_id))
    {
        return C_ERR;
    }

    return C_OK;
}

static void rdmaReleaseResource(RdmaContext *ctx)
{
    rdmaDestroyIoBuf(ctx);

    if (ctx->send_cq)
    {
        ibv_destroy_cq(ctx->send_cq);
    }

    if (ctx->recv_cq)
    {
        ibv_destroy_cq(ctx->recv_cq);
    }

    if (ctx->pd)
    {
        ibv_dealloc_pd(ctx->pd);
    }
}

// proceed to establish connection
static int rdmaHandleConnect(char *err, struct rdma_cm_event *ev, char *ip, size_t ip_len, int *port)
{
    int ret = C_OK;
    struct rdma_cm_id *cm_id = ev->id;
    struct sockaddr_storage caddr;
    RdmaContext *ctx = NULL;
    struct rdma_conn_param conn_param = {
        .responder_resources = 0,
        .initiator_depth = 0,
    };

    memcpy(&caddr, &cm_id->route.addr.dst_addr, sizeof(caddr));
    if (caddr.ss_family == AF_INET)
    {
        struct sockaddr_in *s = (struct sockaddr_in *)&caddr;
        if (ip)
            inet_ntop(AF_INET, (void *)&(s->sin_addr), ip, ip_len);
        if (port)
            *port = ntohs(s->sin_port);
    }
    else
    {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&caddr;
        if (ip)
            inet_ntop(AF_INET6, (void *)&(s->sin6_addr), ip, ip_len);
        if (port)
            *port = ntohs(s->sin6_port);
    }

    ctx = calloc(1, sizeof(RdmaContext));
    ctx->ip = strdup(ip);
    ctx->port = *port;
    cm_id->context = ctx;
    if (rdmaCreateResource(ctx, cm_id) == C_ERR)
    {
        goto reject;
    }

    conn_param.qp_num = cm_id->qp->qp_num;
    ret = rdma_accept(cm_id, &conn_param);
    if (ret)
    {
        serverNetError(err, "RDMA: accept failed");
        goto free_rdma;
    }

    for (int i = 0; i < REDIS_MAX_SGE; i++)
    {

        if (rdmaPostRecv(ctx, cm_id, i) == C_ERR)
        {
            serverLog(LL_WARNING, "RDMA: post recv failed");
            // goto destroy_iobuf;
        }
    }

    serverLog(LL_VERBOSE, "RDMA: successful connection from %s:%d\n", ctx->ip, ctx->port);
    return C_OK;

free_rdma:
    rdmaReleaseResource(ctx);
reject:
    /* reject connect request if hitting error */
    rdma_reject(cm_id, NULL, 0);

    return C_ERR;
}

int rdmaAccept(char *err, char *ip, size_t ip_len, int *port, void **priv)
{
    struct rdma_cm_event *ev;
    enum rdma_cm_event_type ev_type;
    int ret = C_OK;

    ret = rdma_get_cm_event(listen_channel, &ev);
    if (ret)
    {
        if (errno != EAGAIN)
        {
            serverLog(LL_WARNING, "RDMA: listen channel rdma_get_cm_event failed, %s", strerror(errno));
            return ANET_ERR;
        }
        return ANET_OK;
    }

    ev_type = ev->event;
    // severLog(LL_DEBUG, "RDMA: event type %s", rdma_event_str(ev_type));
    switch (ev_type)
    {
    case RDMA_CM_EVENT_CONNECT_REQUEST:
        ret = rdmaHandleConnect(err, ev, ip, ip_len, port);
        if (ret == C_OK)
        {
            *priv = ev->id;
            // todo: magic number as a placeholder for file descriptor
            ret = 0xff;
        }
        break;

    case RDMA_CM_EVENT_ESTABLISHED:
        ret = rdmaHandleEstablished(ev);
        break;

    case RDMA_CM_EVENT_UNREACHABLE:
    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_REJECTED:
    case RDMA_CM_EVENT_ADDR_CHANGE:
    case RDMA_CM_EVENT_DISCONNECTED:
    case RDMA_CM_EVENT_TIMEWAIT_EXIT:
        rdmaHandleDisconnect(ev);
        ret = C_OK;
        break;

    case RDMA_CM_EVENT_MULTICAST_JOIN:
    case RDMA_CM_EVENT_MULTICAST_ERROR:
    case RDMA_CM_EVENT_DEVICE_REMOVAL:
    case RDMA_CM_EVENT_ADDR_RESOLVED:
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
    case RDMA_CM_EVENT_CONNECT_RESPONSE:
    default:
        serverLog(LL_NOTICE, "RDMA: listen channel ignore event: %s", rdma_event_str(ev_type));
        break;
    }

    if (rdma_ack_cm_event(ev))
    {
        serverLog(LL_WARNING, "ack cm event failed\n");
        return ANET_ERR;
    }

    return ret;
}

void acceptRdmaHandler()
{
    serverLog(LL_VERBOSE, "trigger RDMA accept handler\n");
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    void *connpriv = NULL;

    while (max--)
    {
        cfd = rdmaAccept(server.neterr, cip, sizeof(cip), &cport, &connpriv);
        if (cfd == ANET_ERR)
        {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                          "RDMA Accepting client connection: %s", server.neterr);
            return;
        }
        else if (cfd == ANET_OK)
            continue;

        // note, skip it for now
        // anetCloexec(cfd);
        serverLog(LL_VERBOSE, "RDMA Accepted %s:%d", cip, cport);
        // todo: where to accept the connection?

        server.rdma_conn->cm_id = connpriv;
    }
}

void connRdmaEventHandler(void *clientData)
{
    // serverLog(LL_VERBOSE, "RDMA: connRdmaEventHandler\n");
    rdma_connection *rdma_conn = (rdma_connection *)clientData;

    if (connRdmaHandleCq(rdma_conn, true) == C_ERR || connRdmaHandleCq(rdma_conn, false) == C_ERR)
    {
        return;
    }
}

// let's not use non-blocking mode for now
// since we are pulling the events manually anyways
// if (anetNonBlock(NULL, cm_channel->fd) != C_OK)
// {
//     serverLog(LL_WARNING, "RDMA: set cm channel fd non-block failed");
//     goto out;
// }

static void connRdmaClose(rdma_connection *rdma_conn)
{
    // skip for now
    return;
}

static inline uint32_t rdmaRead(RdmaContext *ctx, void *buf, size_t buf_len)
{
    uint32_t toread = MIN(ctx->outstanding_msg_size, buf_len);
    memcpy(buf, ctx->recv_buf + ctx->recv_offset, toread);

    ctx->recv_offset = 0;
    ctx->outstanding_msg_size = 0;

    return toread;
}

static int connRdmaRead(rdma_connection *rdma_conn, void *buf, size_t buf_len)
{
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;

    // make sure the message is ready
    assert(ctx->outstanding_msg_size != 0);

    return rdmaRead(ctx, buf, buf_len);
}

static int rdmaServer(char *err, int port, char *bindaddr, int af, int index)
{
    int s = ANET_OK, rv, afonly = 1;
    char _port[6]; /* strlen("65535") */
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage sock_addr;
    struct rdma_cm_id *listen_cmid;

    if (ibv_fork_init())
    {
        serverLog(LL_WARNING, "RDMA: FATAL error, recv corrupted cmd");
        return ANET_ERR;
    }

    snprintf(_port, 6, "%d", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = af;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE; /* No effect if bindaddr != NULL */
    if (bindaddr && !strcmp("*", bindaddr))
        bindaddr = NULL;

    if (af == AF_INET6 && bindaddr && !strcmp("::*", bindaddr))
        bindaddr = NULL;

    if ((rv = getaddrinfo(bindaddr, _port, &hints, &servinfo)) != 0)
    {
        serverNetError(err, "RDMA: %s", gai_strerror(rv));
        return ANET_ERR;
    }
    else if (!servinfo)
    {
        serverNetError(err, "RDMA: get addr info failed");
        s = ANET_ERR;
        goto end;
    }

    for (p = servinfo; p != NULL; p = p->ai_next)
    {
        memset(&sock_addr, 0, sizeof(sock_addr));
        if (p->ai_family == AF_INET6)
        {
            memcpy(&sock_addr, p->ai_addr, sizeof(struct sockaddr_in6));
            ((struct sockaddr_in6 *)&sock_addr)->sin6_family = AF_INET6;
            ((struct sockaddr_in6 *)&sock_addr)->sin6_port = htons(port);
        }
        else
        {
            memcpy(&sock_addr, p->ai_addr, sizeof(struct sockaddr_in));
            ((struct sockaddr_in *)&sock_addr)->sin_family = AF_INET;
            ((struct sockaddr_in *)&sock_addr)->sin_port = htons(port);
        }

        if (rdma_create_id(listen_channel, &listen_cmid, NULL, RDMA_PS_TCP))
        {
            serverNetError(err, "RDMA: create listen cm id error");
            return ANET_ERR;
        }

        rdma_set_option(listen_cmid, RDMA_OPTION_ID, RDMA_OPTION_ID_AFONLY,
                        &afonly, sizeof(afonly));

        if (rdma_bind_addr(listen_cmid, (struct sockaddr *)&sock_addr))
        {
            serverNetError(err, "RDMA: bind addr error");
            goto error;
        }

        if (rdma_listen(listen_cmid, 0))
        {
            serverNetError(err, "RDMA: listen addr error");
            goto error;
        }

        listen_cmids[index] = listen_cmid;
        printf("RDMA: server start to listen on %s:%d, with index%d\n", bindaddr, port, index);
        goto end;
    }

error:
    if (listen_cmid)
        rdma_destroy_id(listen_cmid);
    s = ANET_ERR;

end:
    freeaddrinfo(servinfo);
    return s;
}

int listenToRdma(int port)
{
    int j, index = 0, ret;
    char **bindaddr = server.bindaddr;
    int bindaddr_count = server.bindaddr_count;
    char *default_bindaddr[2] = {"*", "-::*"};

    assert(server.proto_max_bulk_len <= 512ll * 1024 * 1024);

    /* Force binding of 0.0.0.0 if no bind address is specified. */
    if (server.bindaddr_count == 0)
    {
        bindaddr_count = 2;
        bindaddr = default_bindaddr;
    }

    listen_channel = rdma_create_event_channel();
    if (!listen_channel)
    {
        serverLog(LL_WARNING, "RDMA: Could not create event channel");
        return C_ERR;
    }

    for (j = 0; j < bindaddr_count; j++)
    {
        char *addr = bindaddr[j];
        int optional = *addr == '-';

        if (optional)
            addr++;
        if (strchr(addr, ':'))
        {
            /* Bind IPv6 address. */
            ret = rdmaServer(server.neterr, port, addr, AF_INET6, index);
        }
        else
        {
            /* Bind IPv4 address. */
            ret = rdmaServer(server.neterr, port, addr, AF_INET, index);
            serverLog(LL_VERBOSE, "RDMA: server start to listen on %s:%d\n", addr, port);
        }

        if (ret == ANET_ERR)
        {
            int net_errno = errno;
            serverLog(LL_WARNING, "RDMA: Could not create server for %s:%d: %s",
                      addr, port, server.neterr);

            if (net_errno == EADDRNOTAVAIL && optional)
                continue;

            if (net_errno == ENOPROTOOPT || net_errno == EPROTONOSUPPORT ||
                net_errno == ESOCKTNOSUPPORT || net_errno == EPFNOSUPPORT ||
                net_errno == EAFNOSUPPORT)
                continue;

            return C_ERR;
        }

        index++;
    }

    // note: make it non-blocking
    int flags = fcntl(listen_channel->fd, F_GETFL, 0);
    fcntl(listen_channel->fd, F_SETFL, flags | O_NONBLOCK);

    return C_OK;
}

int main(int argc, char **argv)
{
    if (argc != 3)
    {
        printf("usage: ./server <ip> <port>\n");
        return -1;
    }

    const char *ip = argv[1];
    int port = atoi(argv[2]);

    server.bindaddr_count = 1;
    server.bindaddr[0] = strdup(ip);
    server.port = port;
    server.connected = false;
    server.rdma_conn = calloc(1, sizeof(rdma_connection));

    if (listenToRdma(port) == C_ERR)
    {
        serverLog(LL_WARNING, "RDMA: listen to rdma failed");
        return -1;
    }

    while (server.connected != true)
    {
        struct pollfd pfd;
        pfd.fd = listen_channel->fd;
        pfd.events = POLLIN;
        pfd.revents = 0;

        int poll_ret = poll(&pfd, 1, 1);
        if (poll_ret == 0)
        {
            continue;
        }
        else if (poll_ret < 0)
        {
            printf("poll error: %s\n", strerror(errno));
        }
        else
        {
            acceptRdmaHandler();
        }
    }

    while (true)
    {
        connRdmaEventHandler(server.rdma_conn);
    }

    return 0;
}