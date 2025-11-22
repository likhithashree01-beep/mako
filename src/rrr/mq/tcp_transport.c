#include "tcp_transport.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// TCP transport context (can be expanded as needed)
typedef struct {
    int dummy; // placeholder
} tcp_transport_ctx_t;

static int tcp_connect(struct transport* t, struct rpc_comm_t* comm) {
    // TCP connect logic for both client and server
    apr_status_t status = APR_SUCCESS;
    // If comm->ip is NULL or empty, treat as server bind/listen
    if (!comm->ip[0]) {
        // Server bind/listen
        status = apr_sockaddr_info_get(&comm->sa, NULL, APR_INET, comm->port, 0, comm->mp);
        if (status != APR_SUCCESS) return -1;
        status = apr_socket_create(&comm->s, comm->sa->family, SOCK_STREAM, APR_PROTO_TCP, comm->mp);
        if (status != APR_SUCCESS) return -1;
        status = apr_socket_opt_set(comm->s, APR_SO_NONBLOCK, 1);
        if (status != APR_SUCCESS) return -1;
        status = apr_socket_timeout_set(comm->s, -1);
        if (status != APR_SUCCESS) return -1;
        status = apr_socket_opt_set(comm->s, APR_SO_REUSEADDR, 1);
        if (status != APR_SUCCESS) return -1;
        status = apr_socket_opt_set(comm->s, APR_TCP_NODELAY, 1);
        if (status != APR_SUCCESS) return -1;
        status = apr_socket_bind(comm->s, comm->sa);
        if (status != APR_SUCCESS) return -1;
        status = apr_socket_listen(comm->s, 30000);
        if (status != APR_SUCCESS) return -1;
        return 0;
    } else {
        // Client connect
        status = apr_sockaddr_info_get(&comm->sa, comm->ip, APR_INET, comm->port, 0, comm->mp);
        if (status != APR_SUCCESS) return -1;
        status = apr_socket_create(&comm->s, comm->sa->family, SOCK_STREAM, APR_PROTO_TCP, comm->mp);
        if (status != APR_SUCCESS) return -1;
        status = apr_socket_opt_set(comm->s, APR_TCP_NODELAY, 1);
        if (status != APR_SUCCESS) return -1;
        while (1) {
            status = apr_socket_connect(comm->s, comm->sa);
            if (status == APR_SUCCESS) {
                break;
            } else if (status == APR_ECONNREFUSED) {
                // Retry
            } else if (status == APR_EINVAL) {
                return -1;
            } else {
                return -1;
            }
            apr_sleep(50 * 1000);
        }
        status = apr_socket_opt_set(comm->s, APR_SO_NONBLOCK, 1);
        if (status != APR_SUCCESS) return -1;
        return 0;
    }
}

static int tcp_disconnect(struct transport* t, struct rpc_comm_t* comm) {
    // TCP disconnect logic (close socket)
    if (comm && comm->s) {
        apr_socket_close(comm->s);
        comm->s = NULL;
    }
    return 0;
}

static int tcp_send(struct transport* t, struct rpc_comm_t* comm, const uint8_t* data, size_t len) {
    // TCP send logic (like buf_to_sock), works for both client and server
    if (!comm || !comm->s) return -1;
    apr_status_t status = APR_SUCCESS;
    size_t n = len;
    status = apr_socket_send(comm->s, (const char*)data, &n);
    if (status == APR_SUCCESS || status == APR_EAGAIN) {
        return (int)n;
    }
    return -1;
}

static int tcp_recv(struct transport* t, struct rpc_comm_t* comm, uint8_t* buf, size_t len) {
    // TCP recv logic (like buf_from_sock), works for both client and server
    if (!comm || !comm->s) return -1;
    apr_status_t status = APR_SUCCESS;
    size_t n = len;
    status = apr_socket_recv(comm->s, (char*)buf, &n);
    if (status == APR_SUCCESS || status == APR_EAGAIN) {
        return (int)n;
    }
    return -1;
}

static void tcp_destroy(struct transport* t) {
    if (t->ctx) free(t->ctx);
    free(t);
}

transport_t* create_tcp_transport() {
    transport_t* t = (transport_t*)malloc(sizeof(transport_t));
    t->ctx = malloc(sizeof(tcp_transport_ctx_t));
    t->connect = tcp_connect;
    t->disconnect = tcp_disconnect;
    t->send = tcp_send;
    t->recv = tcp_recv;
    t->destroy = tcp_destroy;
    return t;
}
