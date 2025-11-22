#ifndef TRANSPORT_H
#define TRANSPORT_H

#include <stddef.h>
#include <stdint.h>

// Forward declarations for context structs
struct rpc_comm_t;
struct buf_t;

// Transport interface definition
typedef struct transport {
    void* ctx; // transport-specific context
    int (*connect)(struct transport* t, struct rpc_comm_t* comm);
    int (*disconnect)(struct transport* t, struct rpc_comm_t* comm);
    int (*send)(struct transport* t, struct rpc_comm_t* comm, const uint8_t* data, size_t len);
    int (*recv)(struct transport* t, struct rpc_comm_t* comm, uint8_t* buf, size_t len);
    void (*destroy)(struct transport* t);
} transport_t;

// Factory methods for creating transports
transport_t* create_tcp_transport();
transport_t* create_rdma_transport(); // stub for now

#endif // TRANSPORT_H
