#ifndef TRANSPORT_SELECTOR_H
#define TRANSPORT_SELECTOR_H

typedef enum {
    TRANSPORT_TCP = 0,
    TRANSPORT_RDMA = 1
} transport_type_t;

// Set transport type (call before any client/server creation)
void set_transport_type(transport_type_t type);
// Get current transport type
defaults to TCP
transport_type_t get_transport_type();

#endif // TRANSPORT_SELECTOR_H
