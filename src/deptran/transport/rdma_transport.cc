#include "rdma_transport.h"
#include "../config.h"

namespace janus {

RdmaTransport::RdmaTransport(FastTransport* ft) : fast_transport_(ft) {
}

RdmaTransport::~RdmaTransport() {
}

void RdmaTransport::SendMessage(siteid_t dest, const TransportMessage& msg, TransportCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t reqId = next_req_id_++;
    pending_requests_[reqId] = cb;

    Marshal m;
    m << reqId << msg.type;
    m << MarshallDeputy(msg.payload);

    size_t sz = m.content_size();
    
    // AllocRequestBuffer(reqLen, respLen). 0 for respLen means default max.
    char* buf = fast_transport_->GetRequestBuf(sz, 0);
    
    if (!buf) {
        Log_error("RdmaTransport: Failed to allocate buffer");
        pending_requests_.erase(reqId);
        return;
    }

    m.read(buf, sz);

    auto* config = Config::GetConfig();
    // Assuming Config::GetConfig() returns a valid config pointer
    if (!config) {
         Log_error("RdmaTransport: Config is null");
         pending_requests_.erase(reqId);
         return;
    }

    auto& site_info = config->SiteById(dest);
    
    // We pass 'this' as TransportReceiver* so ReceiveResponse calls us back.
    // We use 0 as reqType for the transport layer, encoding actual type in payload.
    bool success = fast_transport_->SendRequestToShard(this, 
                                                       0, 
                                                       site_info.partition_id_, 
                                                       site_info.id, 
                                                       sz);
    if (!success) {
        Log_error("RdmaTransport: Send failed");
        pending_requests_.erase(reqId);
    }
}

void RdmaTransport::ReceiveResponse(uint8_t reqType, char *respBuf) {
    if (!respBuf) return;

    // Assumes respBuf starts with reqId (uint64_t)
    // We cast directly because we controlled the serialization.
    uint64_t reqId = *(uint64_t*)respBuf;
    
    TransportCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = pending_requests_.find(reqId);
        if (it == pending_requests_.end()) {
            // This might happen for timed out requests or duplicates
            // Log_warn("RdmaTransport: Unknown reqId %lu", reqId);
            return;
        }
        cb = it->second;
        pending_requests_.erase(it);
    }
    
    // Extract payload. Skip reqId.
    char* payload_ptr = respBuf + sizeof(uint64_t);
    
    // Since we don't have the exact size from FastTransport, we must estimate.
    // For safety in this POC, we assume a reasonably large buffer size 
    // that covers expected responses. 
    // eRPC packets are usually MTU sized or larger if zero-copy.
    // We construct a Marshal that COPIES data. 
    size_t estimated_size = 8192; // 8KB
    Marshal m;
    m.write(payload_ptr, estimated_size);
    
    cb(m);
}

}
