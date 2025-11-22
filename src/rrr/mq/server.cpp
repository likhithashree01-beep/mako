
#include "rpc_comm.h"
#include "server.h"

extern poll_mgr_t *mgr_;

void server_create(server_t **svr, poll_mgr_t *mgr) {
    *svr = (server_t*) malloc(sizeof(server_t));
    server_t *s = *svr;
    rpc_common_create(&s->comm);
    poll_job_create(&s->pjob);
    s->pjob->do_read = &handle_server_accept;
    s->pjob->holder = s;
    s->pjob->mgr = (mgr != NULL) ? mgr: mgr_;

    s->is_start = false;
    mpr_hash_create(&s->ht_conn);

    // Select transport at runtime
    #include "transport_selector.h"
    extern transport_t* create_tcp_transport();
    extern transport_t* create_rdma_transport();
    transport_type_t ttype = get_transport_type();
    if (ttype == TRANSPORT_RDMA) {
        s->transport = create_rdma_transport();
    } else {
        s->transport = create_tcp_transport();
    }
    s->comm->transport = s->transport;

    apr_thread_pool_create(&s->tp, 10, 10, s->comm->mp);
}

void server_destroy(server_t *svr) {
    rpc_common_destroy(svr->comm);
    // TODO maybe destroy all connections first?
    mpr_hash_destroy(svr->ht_conn);
    apr_thread_pool_destroy(svr->tp);
    free(svr);
}

void server_bind_listen(server_t *svr) {
    // Use transport abstraction for bind/listen
    int rc = svr->transport->connect(svr->transport, svr->comm); // connect = bind/listen for server
    SAFE_ASSERT(rc == 0);
    LOG_INFO("successfuly bind and listen on port %d.", svr->comm->port);
}

void server_start(server_t *svr) {
    server_t *s = svr;
    server_bind_listen(s);
    apr_pollfd_t pfd = {s->comm->mp, APR_POLL_SOCKET, APR_POLLIN, 0, {NULL}, NULL};
    pfd.desc.s = svr->comm->s;
    pfd.client_data = svr->pjob;
    svr->pjob->pfd = pfd;
    // TODO add the poll job instead?
    poll_mgr_add_job(svr->pjob->mgr, svr->pjob);
//    apr_pollset_add(svr->job->ps, &pfd);    
}

void server_stop(server_t *svr);

void server_reg(server_t *svr, msgid_t msgid, void* fun) {
    LOG_TRACE("server regisger function, msg type:%x", (int32_t)msgid);
    SAFE_ASSERT(fun != NULL);
    mpr_hash_set(svr->comm->ht, &msgid, SZ_MSGID, 
		     &fun, sizeof(void*));
}

void sconn_create(sconn_t **sconn, server_t *svr) {
    *sconn = (sconn_t *) malloc(sizeof(sconn_t));
    sconn_t *sc = *sconn;
    sc->tp = svr->tp;

    rpc_common_create(&sc->comm);
    sc->comm->ht = svr->comm->ht; // TODO should be copy.
    sc->transport = svr->transport; // inherit transport from server

    poll_job_create(&sc->pjob);

    sc->pjob->do_read = handle_sconn_read;
    sc->pjob->do_write = handle_sconn_write;
    sc->pjob->holder = sc;

    sc->pjob->mgr = svr->pjob->mgr;

    buf_create(&sc->buf_recv);
    buf_create(&sc->buf_send);
}

void sconn_destroy(sconn_t *sconn) {
    free(sconn->pjob);
    buf_destroy(sconn->buf_recv);
    buf_destroy(sconn->buf_send);
}

void handle_server_accept(void* arg) {
    server_t *svr = (server_t *) arg;

    apr_status_t status = APR_SUCCESS;
    apr_socket_t *ns = NULL;
    
    LOG_TRACE("on new connection");

    status = apr_socket_accept(&ns, svr->comm->s, svr->comm->mp);

//    apr_socket_t *sock_listen = r->com.s;
//    LOG_INFO("accept on fd %x", sock_listen->socketdes);
    LOG_INFO("accept new connection");

    if (status == APR_EMFILE) {
	LOG_ERROR("cannot open more file handles, please check system configurations.");
	SAFE_ASSERT(0);
    } else if (status == APR_ENFILE) {
	LOG_ERROR("cannot open more file handles, please check system configurations.");
	SAFE_ASSERT(0);
    }

    if (status != APR_SUCCESS) {
        LOG_ERROR("recvr accept error.");
        LOG_ERROR("%s", apr_strerror(status, (char*)malloc(100), 100));
        SAFE_ASSERT(status == APR_SUCCESS);
    }

    apr_socket_opt_set(ns, APR_SO_NONBLOCK, 1);
    apr_socket_opt_set(ns, APR_TCP_NODELAY, 1);
//    apr_socket_opt_set(ns, APR_SO_REUSEADDR, 1);

    sconn_t *sconn;
    sconn_create(&sconn, svr);

    apr_pollfd_t pfd = {svr->comm->mp, APR_POLL_SOCKET, APR_POLLIN, 0, {NULL}, NULL};
    pfd.desc.s = ns;
    pfd.client_data = sconn->pjob;
    sconn->pjob->pfd = pfd;
    sconn->pjob->mgr = svr->pjob->mgr;
    poll_mgr_add_job(sconn->pjob->mgr, sconn->pjob);
}

void* APR_THREAD_FUNC msg_call(apr_thread_t *th, void* arg) {
    rpc_state *state = (rpc_state*)arg;
    // this is where i run it in a new thread or just in the poll thread.
    rpc_state* (**fun)(void*) = NULL;
    size_t sz;
    sconn_t *sconn = state->sconn;
    msgid_t msgid = state->msgid;
    mpr_hash_get(sconn->comm->ht, &msgid, SZ_MSGID, (void**)&fun, &sz);
    SAFE_ASSERT(fun != NULL);
    LOG_TRACE("going to call function %x", *fun);
	   
    (**fun)(state);

    // write back to this connection.	    
    if (state->raw_output) {
	reply_to(state);
	free(state->raw_output);
    }
    free(state->raw_input);
    free(state);
    // the above should be in a seperate function call.
    return NULL;
}

void handle_sconn_read(void* arg) {
    LOG_TRACE("sconn read handle read.");

    sconn_t *sconn = (sconn_t *) arg;
    buf_t *buf = sconn->buf_recv;
    // Use transport abstraction for recv
    int status = sconn->transport->recv(sconn->transport, sconn->comm, buf->raw + buf->idx_write, buf->sz - buf->idx_write);
    if (status > 0) {
        buf->idx_write += status;
    }

    // invoke msg handling.
    size_t sz_c = 0;
    while ((sz_c = buf_sz_content(buf)) > SZ_SZMSG) {
	uint32_t sz_msg = 0;
	SAFE_ASSERT(buf_peek(buf, (uint8_t*)&sz_msg, SZ_SZMSG) == SZ_SZMSG);
	SAFE_ASSERT(sz_msg > 0);
	if (sz_c >= sz_msg + SZ_SZMSG + SZ_MSGID) {
	    SAFE_ASSERT(buf_read(buf, (uint8_t*)&sz_msg, SZ_SZMSG) == SZ_SZMSG);
	    msgid_t msgid = 0;
	    SAFE_ASSERT(buf_read(buf, (uint8_t*)&msgid, SZ_MSGID) == SZ_MSGID);
    	    LOG_TRACE("next message size: %d, type: %x", (int32_t)sz_msg, (int32_t)msgid);

	    rpc_state *state = (rpc_state*)malloc(sizeof(rpc_state));
            state->sz_input = sz_msg;
            state->raw_input = (uint8_t*)malloc(sz_msg);
	    state->raw_output = NULL;
	    state->sz_output = 0;
            state->sconn = sconn;
	    state->msgid = msgid;

	    SAFE_ASSERT(buf_read(buf, (uint8_t*)state->raw_input, sz_msg) == sz_msg);
	    //            apr_thread_pool_push(tp_on_read_, (*(ctx->on_recv)), (void*)state, 0, NULL);
//            mpr_thread_pool_push(tp_read_, (void*)state);
//            apr_atomic_inc32(&n_data_recv_);
            //(*(ctx->on_recv))(NULL, state);
            // FIXME call

	    if (msgid & NEW_THREAD_CALL) {
		apr_thread_pool_t *tp = sconn->tp;
		SAFE_ASSERT(tp != NULL);
		apr_thread_pool_push(tp, msg_call, state, 0, NULL);
	    } else {
		msg_call(NULL, state);
	    }
	    
	} else {
	    break;
	}
    }

    if (status == APR_SUCCESS) {
	
    } else if (status == APR_EOF) {
        LOG_INFO("sconn poll on read, received eof, close socket");
	poll_mgr_remove_job(sconn->pjob->mgr, sconn->pjob);
    } else if (status == APR_ECONNRESET) {
        LOG_INFO("on read. connection reset, close socket");
	poll_mgr_remove_job(sconn->pjob->mgr, sconn->pjob);
        // TODO [improve] you may retry connect
    } else if (status == APR_EAGAIN) {
        LOG_TRACE("sconn poll on read, socket busy, resource temporarily unavailable.");
        // do nothing.
    } else {
        LOG_ERROR("unkown error on poll reading. %s\n", 
		  apr_strerror(status, (char*)malloc(100), 100));
        SAFE_ASSERT(0);
    }
}

void handle_sconn_write(void* arg) {
    sconn_t *sconn = (sconn_t*)arg;
    apr_thread_mutex_lock(sconn->comm->mx);
    buf_t *buf = sconn->buf_send;
    int sent = sconn->transport->send(sconn->transport, sconn->comm, buf->raw + buf->idx_read, buf->idx_write - buf->idx_read);
    if (sent > 0) {
        buf->idx_read += sent;
    }
    int mode = (buf_sz_cnt(buf) > 0) ? (APR_POLLIN | APR_POLLOUT) : APR_POLLIN;
    poll_mgr_update_job(sconn->pjob->mgr, sconn->pjob, mode);
    apr_thread_mutex_unlock(sconn->comm->mx);
}

void write_trigger_poll(rpc_comm_t *comm, 
			poll_job_t* pjob, 
			buf_t *buf, 
			msgid_t msgid, 
			const uint8_t *data, 
			size_t sz_data) {
    apr_thread_mutex_lock(comm->mx);
    
//    apr_atomic_add32(&sz_data_tosend_, sizeof(funid_t) + sz_buf + sizeof(size_t));
//    apr_atomic_inc32(&n_data_sent_);
    
    // realloc the write buf if not enough.
    buf_readjust(buf, sz_data + SZ_SZMSG + SZ_MSGID);

    // copy memory
    LOG_TRACE("add message to sending buffer, message size: %d, message type: %x", (int32_t)sz_data, (int32_t)msgid);
     
    //    LOG_TRACE("size in buf:%llx, original size:%llx", 
    //        *(ctx->buf_send.buf + ctx->buf_send.offset_end), sz_buf + sizeof(funid_t));
    
    size_t n = SZ_SZMSG;
    SAFE_ASSERT(buf_write(buf, (uint8_t*)&sz_data, n) == n);

    SAFE_ASSERT(msgid != 0); // message id cannot be zero
    n = SZ_MSGID;
    SAFE_ASSERT(buf_write(buf, (uint8_t*)&msgid, n) == n);

    n = sz_data;
    SAFE_ASSERT(buf_write(buf, (uint8_t*)data, n) == n);

    // change poll type
    poll_mgr_update_job(pjob->mgr, pjob, APR_POLLIN | APR_POLLOUT);
    
    apr_thread_mutex_unlock(comm->mx);
}

void reply_to(rpc_state_t *state) {
    write_trigger_poll(state->sconn->comm,
		      state->sconn->pjob,
		      state->sconn->buf_send,
		      state->msgid,
		      state->raw_output, 
		      state->sz_output);
}


