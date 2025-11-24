#pragma once
#include <string>
#include <memory>
#include <map>
#include "transport_interface.h"
#include "tcp_transport.h"
#include "rdma_transport.h"

// Forward declarations
class FastTransport;

namespace janus {

class ClassicProxy;

class TransportFactory {
public:
    static std::unique_ptr<TransportInterface> CreateTransport(
        const std::string& type, 
        std::map<siteid_t, ClassicProxy*>* proxies,
        FastTransport* ft = nullptr) 
    {
        if (type == "rdma" && ft) {
            return std::make_unique<RdmaTransport>(ft);
        }
        // Default to TCP
        return std::make_unique<TcpTransport>(proxies);
    }
};

}
