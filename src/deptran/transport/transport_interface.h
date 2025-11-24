#pragma once

#include "../constants.h"
#include "../__dep__.h"
#include "../marshal-value.h"

namespace janus {

struct TransportMessage {
    uint8_t type;
    std::shared_ptr<Marshallable> payload;
};

enum MessageType {
    MSG_BULK_ACCEPT = 1,
    MSG_BULK_PREPARE,
    MSG_HEARTBEAT,
    MSG_SYNC_LOG,
    MSG_SYNC_NOOP,
    MSG_SYNC_COMMIT,
    MSG_BULK_DECIDE
};

// Generic callback that receives the response as a Marshal
// which can be deserialized into specific types.
using TransportCallback = std::function<void(Marshal& reply)>;

class TransportInterface {
public:
    virtual ~TransportInterface() = default;
    
    virtual void SendMessage(siteid_t dest, 
                             const TransportMessage& msg, 
                             TransportCallback cb) = 0;
                             
    virtual void BroadcastMessage(const std::vector<siteid_t>& dests, 
                                  const TransportMessage& msg, 
                                  TransportCallback cb) {
        for (auto dest : dests) {
            SendMessage(dest, msg, cb);
        }
    }
};

} // namespace janus
