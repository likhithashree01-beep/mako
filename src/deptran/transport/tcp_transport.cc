#include "tcp_transport.h"
#include "../procedure.h"
#include "../rcc_rpc.h"
#include <cstdio>

namespace janus {

void TcpTransport::SendMessage(siteid_t dest, const TransportMessage& msg, TransportCallback cb) {
    // fprintf(stderr, "TcpTransport::SendMessage to %d type %d\n", dest, msg.type);
    if (proxies_ == nullptr) {
        Log_error("TcpTransport: proxies_ map is null");
        return;
    }
    // fprintf(stderr, "TcpTransport: proxies_ addr: %p, size: %lu\n", proxies_, proxies_->size());
    
    auto it = proxies_->find(dest);
    if (it == proxies_->end()) {
        Log_error("TcpTransport: No proxy found for site %d", dest);
        return;
    }
    
    auto proxy = (MultiPaxosProxy*) it->second;
    if (proxy == nullptr) {
        Log_error("TcpTransport: Proxy is null for site %d", dest);
        return;
    }
    // fprintf(stderr, "TcpTransport: proxy addr: %p\n", proxy);
    
    FutureAttr fuattr;
    fuattr.callback = [cb](rusty::Arc<Future> fu) {
        if (fu->get_error_code() != 0) {
            // Log error but don't crash?
            // Log_error("TcpTransport: RPC error %d", fu->get_error_code());
            return;
        }
        cb(fu->get_reply());
    };
    
    if (msg.payload == nullptr) {
        Log_error("TcpTransport: msg.payload is null for message type %d", msg.type);
        return;
    }

    MarshallDeputy md(msg.payload);
    // fprintf(stderr, "TcpTransport: MarshallDeputy created. kind: %d\n", md.kind_);
    
    switch (msg.type) {
        case MSG_BULK_ACCEPT:
            // fprintf(stderr, "TcpTransport: calling async_BulkAccept\n");
            proxy->async_BulkAccept(md, fuattr);
            break;
        case MSG_BULK_PREPARE:
            proxy->async_BulkPrepare(md, fuattr);
            break;
        case MSG_HEARTBEAT:
            proxy->async_Heartbeat(md, fuattr);
            break;
        case MSG_SYNC_LOG:
            proxy->async_SyncLog(md, fuattr);
            break;
        case MSG_SYNC_NOOP:
            proxy->async_SyncNoOps(md, fuattr);
            break;
        case MSG_SYNC_COMMIT:
            proxy->async_SyncCommit(md, fuattr);
            break;
        case MSG_BULK_DECIDE:
            proxy->async_BulkDecide(md, fuattr);
            break;
        default:
            Log_error("Unknown message type %d", msg.type);
    }
}

}
