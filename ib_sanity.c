#undef NDEBUG

#include <arpa/inet.h>
#include <assert.h>
#include <malloc.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/mman.h>

#include <rdma/rdma_cma.h>

#define PAGE_SIZE 4096

#define PARAM_EQ_NUM 0
#define PARAM_GID_INDEX 0
#define PARAM_INLINE_SIZE 0
#define PARAM_MAX_RECV_SGE 1
#define PARAM_MAX_SEND_SGE 1
#define PARAM_MIN_RNR_TIMER 12
#define PARAM_MR_LENGTH 16384
#define PARAM_OUT_READS 1
#define PARAM_PORT 1235
#define PARAM_PORT_NUM 1
#define PARAM_QP_TIMEOUT 14
#define PARAM_RX_DEPTH 127
#define PARAM_SL 0
#define PARAM_TRAFFIC_CLASS 0
#define PARAM_TX_DEPTH 127

struct ctx {
	char *send_buf;
	char *recv_buf;
	// struct ibv_context *context;
	struct ibv_cq *recv_cq;
	struct ibv_cq *send_cq;
	// struct ibv_mr *mr;
	struct ibv_mr *send_mr;
	struct ibv_mr *recv_mr;
	struct ibv_pd *pd;
	// struct ibv_qp *qp;
	struct rdma_cm_id *cm_id;
};

static int *payload_lengths;
static int payload_length_count;

static void qp_init(struct ctx *ctx)
{
	struct ibv_qp_init_attr qp_init_attr = {0};
	struct ibv_qp_attr attr = {0};
	int flags;

	// qp_init_attr.cap.max_inline_data = PARAM_INLINE_SIZE;
	qp_init_attr.cap.max_recv_sge = PARAM_MAX_RECV_SGE;
	qp_init_attr.cap.max_recv_wr = PARAM_RX_DEPTH;
	qp_init_attr.cap.max_send_sge = PARAM_MAX_SEND_SGE;
	qp_init_attr.cap.max_send_wr = PARAM_TX_DEPTH;
	qp_init_attr.qp_type = IBV_QPT_RC;
	qp_init_attr.recv_cq = ctx->recv_cq;
	qp_init_attr.send_cq = ctx->send_cq;
	qp_init_attr.srq = NULL;

	ctx->cm_id->qp = ibv_create_qp(ctx->pd, &qp_init_attr);
	assert(ctx->cm_id->qp);

	// note
	attr.pkey_index = 0;
	attr.port_num = PARAM_PORT_NUM;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;
	attr.qp_state = IBV_QPS_INIT;

	flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

	assert (!ibv_modify_qp(ctx->cm_id->qp, &attr, flags));
}

static int get_len(int server_tx, int id)
{
	return payload_lengths[id - 1];
}

static void clear_recv_buf(const struct ctx *ctx)
{
	memset(ctx->recv_buf, '.', PARAM_MR_LENGTH / 2);
}

static void post_recv(const struct ctx *ctx, int is_server, int id)
{
	struct ibv_recv_wr *bad_wr_recv;
	struct ibv_recv_wr rwr = {0};
	struct ibv_sge recv_sge_list = {0};

	clear_recv_buf(ctx);
	// recv_sge_list.addr = (uintptr_t) ctx->recv_buf;
	// recv_sge_list.length = get_len(!is_server, id);
	// recv_sge_list.lkey = ctx->recv_mr->lkey;
	// rwr.next = NULL;
	// rwr.num_sge = PARAM_MAX_RECV_SGE;
	// rwr.sg_list = &recv_sge_list;
	// rwr.wr_id = 0;

	struct ibv_sge sge;
	sge.addr = (uint64_t)(ctx->recv_buf);
	sge.length = get_len(!is_server, id);
	sge.lkey = ctx->recv_mr->lkey;
	rwr.next = NULL;
	rwr.num_sge = PARAM_MAX_RECV_SGE;
	rwr.sg_list = &sge;
	rwr.wr_id = 0;

	assert(!ibv_post_recv(ctx->cm_id->qp, &rwr, &bad_wr_recv));
}

static void rdma_cm_server(struct ctx *ctx, const char *servername)
{
	int is_server = 1;
	struct rdma_cm_event *event;
	struct rdma_conn_param conn_param = {0};
	struct rdma_event_channel *channel;
	struct sockaddr_in addr = {0};

	channel = rdma_create_event_channel();
	assert(channel);

	assert(!rdma_create_id(channel, &ctx->cm_id, NULL, RDMA_PS_TCP));

	addr.sin_family = AF_INET;
	addr.sin_port = htons(PARAM_PORT);
	// note
	assert(inet_aton(servername, &addr.sin_addr));
	assert(!rdma_bind_addr(ctx->cm_id, (struct sockaddr *)&addr));

	assert(!rdma_listen(ctx->cm_id, 0));

	assert(!rdma_get_cm_event(channel, &event));
	assert(event->event == RDMA_CM_EVENT_CONNECT_REQUEST);
	ctx->cm_id = event->id;
	qp_init(ctx);
	post_recv(ctx, is_server, 1);
	conn_param.qp_num = ctx->cm_id->qp->qp_num;
	assert(!rdma_accept(event->id, &conn_param));
	assert(!rdma_ack_cm_event(event));

	assert(!rdma_get_cm_event(channel, &event));
	assert(event->event == RDMA_CM_EVENT_ESTABLISHED);
	assert(!rdma_ack_cm_event(event));
}

static void rdma_cm_client(struct ctx *ctx, const char *servername)
{
	int is_server = 0;
	struct rdma_cm_event *event;
	struct rdma_conn_param conn_param = {0};
	struct rdma_event_channel *channel;
	struct sockaddr_in addr = {0};

	channel = rdma_create_event_channel();
	assert(channel);

	assert(!rdma_create_id(channel, &ctx->cm_id, NULL, RDMA_PS_TCP));

	addr.sin_family = AF_INET;
	addr.sin_port = htons(PARAM_PORT);
	assert(inet_aton(servername, &addr.sin_addr));
	assert(!rdma_resolve_addr(ctx->cm_id, NULL, (struct sockaddr *)&addr, 2000));

	assert(!rdma_get_cm_event(channel, &event));
	assert(event->event == RDMA_CM_EVENT_ADDR_RESOLVED);
	assert(!rdma_resolve_route(ctx->cm_id, 2000));
	assert(!rdma_ack_cm_event(event));

	assert(!rdma_get_cm_event(channel, &event));
	assert(event->event == RDMA_CM_EVENT_ROUTE_RESOLVED);
	ctx->cm_id = event->id;
	qp_init(ctx);
	post_recv(ctx, is_server, 1);

	// conn_param.responder_resources = 0;
	// conn_param.initiator_depth = 0;
	// conn_param.retry_count = 7;
	// conn_param.rnr_retry_count = 7;
	conn_param.qp_num = ctx->cm_id->qp->qp_num;
	assert(!rdma_connect(ctx->cm_id, &conn_param));
	assert(!rdma_ack_cm_event(event));

	assert(!rdma_get_cm_event(channel, &event));
	assert(event->event == RDMA_CM_EVENT_ESTABLISHED);
	assert(!rdma_ack_cm_event(event));
}


static struct ibv_device* find_ib_device(const char *ib_devname)
{
	int num_of_device;
	struct ibv_device **dev_list;
	struct ibv_device *ib_dev = NULL;

	dev_list = ibv_get_device_list(&num_of_device);
	assert(num_of_device > 0);
	for (; (ib_dev = *dev_list); ++dev_list)
		if (!strcmp(ibv_get_device_name(ib_dev), ib_devname))
			break;
	assert(ib_dev);
	return ib_dev;
}

static void get_msg(char *msg, int len, int server_tx, int id)
{
	int i = 0;

	assert(len == get_len(server_tx, id));

	i += snprintf(msg, len - i, "%s-%d ", server_tx ? "server" : "client", id);

	for (; i < len; i += 9)
		snprintf(msg + i, len - i, "%02d:%05d ", id, i);
}

static void rx(const struct ctx *ctx, int is_server, int id)
{
	int ne;
	unsigned int expected_len, len;
	struct ibv_wc wc;
	char expected[PARAM_MR_LENGTH / 2];
	const char *truncated;
	time_t timeout_at;

	memset(&wc, 0xcc, sizeof(wc));
	timeout_at = time(NULL) + 10;
	do {
		ne = ibv_poll_cq(ctx->recv_cq, 1, &wc);
		assert(time(NULL) < timeout_at);
	} while (!ne);
	assert(ne > 0);
	assert(wc.status == IBV_WC_SUCCESS);
	assert(wc.wr_id == 0);

	// printf("rx: wc.opcode=%d\n", wc.opcode);

	expected_len = get_len(!is_server, id);
	assert(wc.byte_len == expected_len);

	assert(expected_len <= sizeof(expected));
	get_msg(expected, expected_len, !is_server, id);

	if (memcmp(ctx->recv_buf, expected, expected_len)) {
		len = expected_len;
		truncated = "";
		if (len > 100) {
			truncated = "...";
			len = 100;
		}
		fprintf(stderr, "expected [%.*s]%s\n", len, expected, truncated);
		fprintf(stderr, "received [%.*s]%s\n", len, ctx->recv_buf, truncated);
		assert(0);
	}
}

static void tx(const struct ctx *ctx, int is_server, int id)
{
	int ne, len;
	struct ibv_send_wr *bad_wr;
	struct ibv_send_wr wr;
	// struct ibv_sge sge_list;
	struct ibv_sge sge;
	struct ibv_wc s_wc;

	// FIXME recv side needs time to post a buffer... SF-652
	usleep(10000);

	len = get_len(is_server, id);
	assert(len <= PARAM_MR_LENGTH / 2);
	get_msg(ctx->send_buf, len, is_server, id);

	sge.addr = (uint64_t)ctx->send_buf;
	sge.lkey = ctx->send_mr->lkey;
	sge.length = len;


	// sge_list.addr = (uintptr_t) ctx->send_buf;
	// sge_list.length = len;
	// sge_list.lkey = ctx->send_mr->lkey;
	wr.next = NULL;
	wr.num_sge = PARAM_MAX_SEND_SGE;
	wr.opcode = IBV_WR_SEND;
	wr.send_flags = IBV_SEND_SIGNALED;
	// wr.sg_list = &sge_list;
	wr.sg_list = &sge;
	wr.sg_list->length = len;
	wr.wr_id = 0;

	assert(!ibv_post_send(ctx->cm_id->qp, &wr, &bad_wr));

	// Prefill with an invalid value to catch ibv_poll_cq if it does not update s_wc.
	s_wc.status = -1;

	do {
		ne = ibv_poll_cq(ctx->send_cq, 1, &s_wc);
	} while (!ne);
	assert(ne > 0);
	printf("tx: s_wc.status=%d\n", s_wc.status);
	// printf("rx: wc.opcode=%d\n", s_wc.opcode);
	assert(s_wc.status == IBV_WC_SUCCESS);
}

static void ping_pong(const struct ctx *ctx, int is_server)
{
	int i, state;
	int tx_cnt = 0;
	int rx_cnt = 0;

	state = is_server ? 1 : 2;

	for (i = 0; i < payload_length_count * 2; i++) {
		switch (state) {
		case 1:
			fprintf(stderr, "waiting to rx: tx_cnt=%d rx_cnt=%d is_server=%d len=%d\n", tx_cnt, rx_cnt, is_server, get_len(!is_server, rx_cnt + 1));
			rx_cnt++;
			rx(ctx, is_server, rx_cnt);
			if (rx_cnt < payload_length_count)
				post_recv(ctx, is_server, rx_cnt + 1);
			state = 2;
			break;
		case 2:
			tx_cnt++;
			fprintf(stderr, "tx:            tx_cnt=%d rx_cnt=%d is_server=%d len=%d\n", tx_cnt, rx_cnt, is_server, get_len(is_server, tx_cnt));
			tx(ctx, is_server, tx_cnt);
			state = 1;
			break;
		default:
			assert(0);
		}
	}

	fprintf(stderr, "tx_cnt=%d rx_cnt=%d is_server=%d\n", tx_cnt, rx_cnt, is_server);
}

static void init(const char *ib_devname, struct ctx *ctx)
{
	struct ibv_device *ib_dev;

	ib_dev = find_ib_device(ib_devname);

	struct ibv_context *ib_context;
	ib_context = ibv_open_device(ib_dev);
	assert(ib_context);

	ctx->pd = ibv_alloc_pd(ib_context);
	assert(ctx->pd);

	ctx->send_cq = ibv_create_cq(ib_context, PARAM_TX_DEPTH, NULL, NULL, PARAM_EQ_NUM);
	assert(ctx->send_cq);

	ctx->recv_cq = ibv_create_cq(ib_context, PARAM_RX_DEPTH, NULL, NULL, PARAM_EQ_NUM);
	assert(ctx->recv_cq);

	ctx->send_buf = mmap(NULL, PARAM_MR_LENGTH, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	assert(ctx->send_buf != MAP_FAILED);

	// ctx->recv_buf = ctx->send_buf + PARAM_MR_LENGTH / 2;
	ctx->recv_buf = mmap(NULL, PARAM_MR_LENGTH, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	assert(ctx->recv_buf != MAP_FAILED);


	ctx->send_mr = ibv_reg_mr(ctx->pd, ctx->send_buf, PARAM_MR_LENGTH, IBV_ACCESS_LOCAL_WRITE);
	ctx->recv_mr = ibv_reg_mr(ctx->pd, ctx->recv_buf, PARAM_MR_LENGTH, IBV_ACCESS_LOCAL_WRITE);

	// assert(ctx->mr);
}


int main(int argc, char *argv[])
{
	const char *ib_devname;
	const char *servername;
	const char *token;
	const char *type;
	char *payloads;
	int sockfd = -1, is_server, rdma_cm;
	struct ctx ctx = {0};
	// struct ibv_port_attr port_attr;

	if (argc != 6) {
		fprintf(stderr, "Usage: %s [server|client] payloads server_ip ib_devname rdma_cm\n", argv[0]);
		return 1;
	}

	type = argv[1];
	payloads = argv[2];
	servername = argv[3];
	ib_devname = argv[4];
	rdma_cm = atoi(argv[5]);

	if (!strcmp(type, "server")) {
		is_server = 1;
	} else if (!strcmp(type, "client")) {
		is_server = 0;
	} else {
		assert(0);
	}

	token = strtok(payloads, ",");
	while (token) {
		payload_lengths = realloc(payload_lengths, (payload_length_count + 1) * sizeof(payload_lengths[0]));
		payload_lengths[payload_length_count] = atoi(token);
		assert(payload_lengths[payload_length_count] > 0);
		payload_length_count++;
		token = strtok(NULL, ",");
	}

	init(ib_devname, &ctx);

	// assert(!ibv_query_port(ctx.context, PARAM_PORT_NUM, &port_attr));
	// assert(port_attr.state == IBV_PORT_ACTIVE);

	if (rdma_cm) {
		if (is_server)
			rdma_cm_server(&ctx, servername);
		else
			rdma_cm_client(&ctx, servername);
	} else {
		// pass
	}

	ping_pong(&ctx, is_server);

	if (sockfd != -1)
		close(sockfd);

	// assert(!ibv_destroy_qp(ctx.qp));
	assert(!ibv_destroy_cq(ctx.cm_id->send_cq));
	assert(!ibv_destroy_cq(ctx.send_cq));
	assert(!ibv_destroy_cq(ctx.recv_cq));
	// assert(!ibv_dereg_mr(ctx.mr));
	assert(!ibv_dealloc_pd(ctx.pd));
	// assert(!ibv_close_device(ctx.context));

	munmap(ctx.send_buf, PARAM_MR_LENGTH);

	return 0;
}