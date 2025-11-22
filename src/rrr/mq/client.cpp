
#include <apr_time.h>
#include "polling.h"
#include "rpc_comm.h"
#include "server.h"
#include "client.h"

extern poll_mgr_t *mgr_;

void client_create(client_t **cli, poll_mgr_t *mgr) {
    *cli = (client_t *) malloc(sizeof(client_t));
    client_t *c = *cli;
    rpc_common_create(&c->comm);
    poll_job_create(&c->pjob);
    c->pjob->do_read = handle_client_read;
    c->pjob->do_write = handle_client_write;
    c->pjob->holder = c;
    c->pjob->mgr = (mgr != NULL) ? mgr : mgr_;

    buf_create(&c->buf_recv);
    buf_create(&c->buf_send);
    // Select transport at runtime
    #include "transport_selector.h"
    extern transport_t* create_tcp_transport();
    extern transport_t* create_rdma_transport();
    transport_type_t ttype = get_transport_type();
    if (ttype == TRANSPORT_RDMA) {
        c->transport = create_rdma_transport();
    } else {
        c->transport = create_tcp_transport();
    }
    c->comm->transport = c->transport;
    LOG_DEBUG("a new client created.");
}

void client_destroy(client_t *cli) {
    if (cli->transport) cli->transport->destroy(cli->transport);
    rpc_common_destroy(cli->comm);
    buf_destroy(cli->buf_recv);
    buf_destroy(cli->buf_send);
    free(cli);
    LOG_DEBUG("a client destroyed");
}

void client_connect(client_t *cli) {
    // Use transport abstraction for connect
    LOG_DEBUG("connecting to server %s %d", cli->comm->ip, cli->comm->port);
    int rc = cli->transport->connect(cli->transport, cli->comm);
    SAFE_ASSERT(rc == 0);
    // Setup poll job as before (assume comm->s is set by transport)
    apr_pollfd_t pfd = {cli->comm->mp, APR_POLL_SOCKET, APR_POLLIN, 0, {NULL}, NULL};
    pfd.desc.s = cli->comm->s;
    pfd.client_data = cli->pjob;
    cli->pjob->pfd = pfd;
    poll_mgr_add_job(cli->pjob->mgr, cli->pjob);
    
   // status = apr_pollset_add(pollset_, &ctx->pfd);
    //  status = apr_pollset_add(ctx->ps, &ctx->pfd);
    //    SAFE_ASSERT(status == APR_SUCCESS);
}

void client_disconnect(client_t *cli) {
    // Use transport abstraction for disconnect
    if (cli->transport) cli->transport->disconnect(cli->transport, cli->comm);
}

void handle_client_read(void *arg) {
    client_t *cli = (client_t*) arg;
    buf_t *buf = cli->buf_recv;
    // Use transport abstraction for recv
    int status = cli->transport->recv(cli->transport, cli->comm, buf->raw + buf->idx_write, buf->sz - buf->idx_write);
    if (status > 0) {
        buf->idx_write += status;
    }

    // invoke msg handling.
    size_t sz_c = 0;
    while ((sz_c = buf_sz_content(buf)) > SZ_SZMSG) {
	uint32_t sz_msg = 0;
	buf_peek(buf, (uint8_t*)&sz_msg, sizeof(sz_msg));
	if (sz_c >= sz_msg + SZ_SZMSG + SZ_MSGID) {
	    buf_read(buf, (uint8_t*)&sz_msg, SZ_SZMSG);
	    msgid_t msgid = 0;
	    buf_read(buf, (uint8_t*)&msgid, SZ_MSGID);

	    rpc_state *state = (rpc_state*)malloc(sizeof(rpc_state));
            state->sz_input = sz_msg;
            state->raw_input = (uint8_t *)malloc(sz_msg);
	    //            state->ctx = ctx;

	    buf_read(buf, state->raw_input, sz_msg);
//            state->ctx = ctx;
/*
            apr_thread_pool_push(tp_on_read_, (*(ctx->on_recv)), (void*)state, 0, NULL);
//            mpr_thread_pool_push(tp_read_, (void*)state);
*/
//            apr_atomic_inc32(&n_data_recv_);
            //(*(ctx->on_recv))(NULL, state);
            // FIXME call

            rpc_state* (**fun)(void*) = NULL;
            size_t sz;
            mpr_hash_get(cli->comm->ht, &msgid, SZ_MSGID, (void**)&fun, &sz);
            SAFE_ASSERT(fun != NULL);
            LOG_TRACE("going to call function %x", *fun);
	    //            ctx->n_rpc++;
	    //            ctx->sz_recv += n;
            (**fun)(state);

            free(state->raw_input);
            free(state);
	} else {
	    break;
	}
    }

    if (status == APR_SUCCESS) {
	
    } else if (status == APR_EOF) {
        LOG_DEBUG("cli poll on read, received eof, close socket");
	poll_mgr_remove_job(cli->pjob->mgr, cli->pjob);
    } else if (status == APR_ECONNRESET) {
        LOG_ERROR("cli poll on read. connection reset.");
	poll_mgr_remove_job(cli->pjob->mgr, cli->pjob);
        // TODO [improve] you may retry connect
    } else if (status == APR_EAGAIN) {
        LOG_DEBUG("cli poll on read. read socket busy, resource temporarily unavailable.");
        // do nothing.
    } else {
        LOG_ERROR("unkown error on poll reading. %s\n", 
		  apr_strerror(status, (char*)malloc(100), 100));
        SAFE_ASSERT(0);
    }
}

void handle_client_write(void *arg) {
    client_t *cli = (client_t*) arg;
    buf_t *buf = cli->buf_send;
    LOG_TRACE("handle client write");
    apr_thread_mutex_lock(cli->comm->mx);
    int sent = cli->transport->send(cli->transport, cli->comm, buf->raw + buf->idx_read, buf->idx_write - buf->idx_read);
    if (sent > 0) {
        buf->idx_read += sent;
    }
    int mode = (buf_sz_cnt(buf) > 0) ? (APR_POLLIN | APR_POLLOUT) : APR_POLLIN;
    if (buf_sz_cnt(buf) == 0) {
        poll_mgr_update_job(cli->pjob->mgr, cli->pjob, mode);
    }
    apr_thread_mutex_unlock(cli->comm->mx);
}

void client_reg(client_t *cli, msgid_t msgid, void* fun) {
    LOG_TRACE("client regisger function, %x", fun);
    mpr_hash_set(cli->comm->ht, &msgid, SZ_MSGID, &fun, sizeof(void*)); 
}

void client_call(client_t *cli, 
		 msgid_t msgid, 
		 const uint8_t *data, 
		 size_t sz_data) {
    write_trigger_poll(cli->comm,
		       cli->pjob,
		       cli->buf_send,
		       msgid,
		       data,
		       sz_data);
}

