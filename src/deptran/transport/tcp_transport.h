#pragma once
#include "transport_interface.h"
#include <map>

namespace janus {

class ClassicProxy; // Forward declaration

class TcpTransport : public TransportInterface {
public:
    // Reference to proxies managed by Communicator
    std::map<siteid_t, ClassicProxy*>* proxies_;

    TcpTransport(std::map<siteid_t, ClassicProxy*>* proxies) : proxies_(proxies) {}

    void SendMessage(siteid_t dest, const TransportMessage& msg, TransportCallback cb) override;
};

}
