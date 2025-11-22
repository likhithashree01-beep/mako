#include "transport_selector.h"
static transport_type_t g_transport_type = TRANSPORT_TCP;

void set_transport_type(transport_type_t type) {
    g_transport_type = type;
}

transport_type_t get_transport_type() {
    return g_transport_type;
}
