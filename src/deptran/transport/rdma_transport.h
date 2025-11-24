#pragma once
#include "transport_interface.h"

// Fix conflict between deptran/constants.h (macro SUCCESS) and mako/lib/common.h (const int SUCCESS)
#ifdef SUCCESS
#undef SUCCESS
#endif

#include "../../mako/lib/fasttransport.h"
#include <unordered_map>
#include <mutex>

namespace janus {

class RdmaTransport : public TransportInterface, public TransportReceiver {
public:
    FastTransport* fast_transport_;
    std::unordered_map<uint64_t, TransportCallback> pending_requests_;
    std::mutex mutex_;
    uint64_t next_req_id_ = 1;

    RdmaTransport(FastTransport* ft);
    ~RdmaTransport();

    void SendMessage(siteid_t dest, const TransportMessage& msg, TransportCallback cb) override;
    
    // TransportReceiver implementation
    void ReceiveResponse(uint8_t reqType, char *respBuf) override;
    
    // We might not need to handle requests if we only use this for Client -> Server logic
    // but Paxos nodes are peers.
    size_t ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf) override {
        // TODO: Implement if this transport receives requests (Server side)
        return 0;
    }
    
    bool Blocked() override { return false; }
};

}
