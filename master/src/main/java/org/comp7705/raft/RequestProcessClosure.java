package org.comp7705.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.google.protobuf.Message;
import org.comp7705.operation.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class RequestProcessClosure implements Closure {

    private static final Logger logger = LoggerFactory.getLogger(RequestProcessClosure.class);

    private static final AtomicIntegerFieldUpdater<RequestProcessClosure> STATE_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(RequestProcessClosure.class, "state");

    private static final int                                              PENDING       = 0;
    private static final int                                              RESPOND       = 1;

    private final Operation operation;
    private final RpcContext rpcCtx;

    private Message                                                           response;

    private volatile int                                                  state         = PENDING;

    public RequestProcessClosure(Operation operation, RpcContext rpcCtx) {
        super();
        this.operation = operation;
        this.rpcCtx = rpcCtx;
    }

    public RpcContext getRpcCtx() {
        return rpcCtx;
    }

    public Operation getOperation() {
        return operation;
    }

    public Message getResponse() {
        return response;
    }

    public void sendResponse(final Message response) {
        this.response = response;
        run(null);
    }

    /**
     * Run the closure and send response.
     */
    @Override
    public void run(final Status status) {
        if (!STATE_UPDATER.compareAndSet(this, PENDING, RESPOND)) {
            logger.warn("A response: {} with status: {} sent repeatedly!", this.response, status);
            return;
        }
        this.rpcCtx.sendResponse(this.response);
    }
}