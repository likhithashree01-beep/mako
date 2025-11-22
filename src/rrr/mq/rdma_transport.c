
#include "rdma_transport.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <infiniband/verbs.h>

// Minimal RDMA transport context
typedef struct {
    struct ibv_context* ctx;
    struct ibv_pd* pd;
    struct ibv_cq* cq;
    struct ibv_qp* qp;
    struct ibv_mr* mr;
    uint8_t* buf;
    size_t buf_size;
    int connected;
} rdma_transport_ctx_t;

// Helper: cleanup
static void rdma_cleanup(rdma_transport_ctx_t* rctx) {
    if (!rctx) return;
    if (rctx->qp) ibv_destroy_qp(rctx->qp);
    if (rctx->cq) ibv_destroy_cq(rctx->cq);
    if (rctx->mr) ibv_dereg_mr(rctx->mr);
    if (rctx->pd) ibv_dealloc_pd(rctx->pd);
    if (rctx->ctx) ibv_close_device(rctx->ctx);
    if (rctx->buf) free(rctx->buf);
    free(rctx);
}

static int rdma_connect(struct transport* t, struct rpc_comm_t* comm) {
    // Minimal: open first available device, create context, pd, cq, qp, mr
    rdma_transport_ctx_t* rctx = calloc(1, sizeof(rdma_transport_ctx_t));
    struct ibv_device** dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        fprintf(stderr, "[RDMA] No RDMA devices found\n");
        return -1;
    }
    rctx->ctx = ibv_open_device(dev_list[0]);
    ibv_free_device_list(dev_list);
    if (!rctx->ctx) {
        fprintf(stderr, "[RDMA] Failed to open device\n");
        rdma_cleanup(rctx);
        return -1;
    }
    rctx->pd = ibv_alloc_pd(rctx->ctx);
    rctx->cq = ibv_create_cq(rctx->ctx, 16, NULL, NULL, 0);
    rctx->buf_size = 4096;
    rctx->buf = malloc(rctx->buf_size);
    rctx->mr = ibv_reg_mr(rctx->pd, rctx->buf, rctx->buf_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    // QP setup (simplified, not real connection)
    struct ibv_qp_init_attr qp_init = {0};
    qp_init.send_cq = rctx->cq;
    qp_init.recv_cq = rctx->cq;
    qp_init.qp_type = IBV_QPT_RC;
    qp_init.cap.max_send_wr = 16;
    qp_init.cap.max_recv_wr = 16;
    qp_init.cap.max_send_sge = 1;
    qp_init.cap.max_recv_sge = 1;
    rctx->qp = ibv_create_qp(rctx->pd, &qp_init);
    if (!rctx->qp) {
        fprintf(stderr, "[RDMA] Failed to create QP\n");
        rdma_cleanup(rctx);
        return -1;
    }
    rctx->connected = 1;
    t->ctx = rctx;
    printf("[RDMA] RDMA context initialized (stub, not real connection)\n");
    return 0;
}

static int rdma_disconnect(struct transport* t, struct rpc_comm_t* comm) {
    rdma_transport_ctx_t* rctx = (rdma_transport_ctx_t*)t->ctx;
    rdma_cleanup(rctx);
    t->ctx = NULL;
    printf("[RDMA] RDMA context cleaned up\n");
    return 0;
}

static int rdma_send(struct transport* t, struct rpc_comm_t* comm, const uint8_t* data, size_t len) {
    rdma_transport_ctx_t* rctx = (rdma_transport_ctx_t*)t->ctx;
    if (!rctx || !rctx->connected) return -1;
    // Minimal: just copy to buffer (no real send)
    size_t n = (len < rctx->buf_size) ? len : rctx->buf_size;
    memcpy(rctx->buf, data, n);
    printf("[RDMA] send stub: copied %zu bytes\n", n);
    return (int)n;
}

static int rdma_recv(struct transport* t, struct rpc_comm_t* comm, uint8_t* buf, size_t len) {
    rdma_transport_ctx_t* rctx = (rdma_transport_ctx_t*)t->ctx;
    if (!rctx || !rctx->connected) return -1;
    // Minimal: just copy from buffer (no real recv)
    size_t n = (len < rctx->buf_size) ? len : rctx->buf_size;
    memcpy(buf, rctx->buf, n);
    printf("[RDMA] recv stub: copied %zu bytes\n", n);
    return (int)n;
}

static void rdma_destroy(struct transport* t) {
    if (t->ctx) rdma_cleanup((rdma_transport_ctx_t*)t->ctx);
    free(t);
}

transport_t* create_rdma_transport() {
    transport_t* t = (transport_t*)malloc(sizeof(transport_t));
    t->ctx = NULL;
    t->connect = rdma_connect;
    t->disconnect = rdma_disconnect;
    t->send = rdma_send;
    t->recv = rdma_recv;
    t->destroy = rdma_destroy;
    return t;
}
