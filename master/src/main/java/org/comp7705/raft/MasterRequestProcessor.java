package org.comp7705.raft;

import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.util.Requires;
import lombok.Getter;
import org.comp7705.MasterServer;
import org.comp7705.common.AddStage;
import org.comp7705.common.RequestType;
import org.comp7705.operation.AddOperation;
import org.comp7705.operation.Operation;
import org.comp7705.protocol.definition.CheckArgs4AddRequest;
import org.comp7705.protocol.definition.CheckArgs4AddResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.comp7705.common.RequestType.CHECK_ARGS_4_ADD_REQUEST;

public class MasterRequestProcessor<T> implements RpcProcessor<T> {

    private static final Logger logger = LoggerFactory.getLogger(MasterRequestProcessor.class);


    @Getter
    private final Class<T> reqClazz;

    private final MasterServer masterServer;

    public MasterRequestProcessor(Class<T> reqClazz, MasterServer masterServer) {
        this.reqClazz = reqClazz;
        this.masterServer = masterServer;
    }


    @Override
    public void handleRequest(RpcContext rpcCtx, T request) {
        Requires.requireNonNull(request, "request");
        logger.info("Receive request {} from {}", request, rpcCtx.getRemoteAddress());
        Operation operation;
        Task task = new Task();
        RequestProcessClosure closure;
        RequestType type = RequestType.getRequestType(request.getClass().getSimpleName());
        switch (Objects.requireNonNull(type)) {
            case CHECK_ARGS_4_ADD_REQUEST:
                logger.info("Receive check args 4 add request.");
                CheckArgs4AddRequest checkArgs4AddRequest = (CheckArgs4AddRequest) request;
                operation = new AddOperation("a", checkArgs4AddRequest.getPath(), checkArgs4AddRequest.getFileName(),
                        checkArgs4AddRequest.getSize(), AddStage.CHECK_ARGS);
                closure = new RequestProcessClosure(operation, rpcCtx);
                break;

            default:
                return;
        }
        try {
            task.setData(ByteBuffer.wrap(serialize(operation)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        task.setDone(closure);
        logger.info("Apply task {}.", task);
        masterServer.getNode().apply(task);
    }

    @Override
    public String interest() {
        return CheckArgs4AddRequest.class.getName();
    }

    private byte[] serialize(Operation operation) throws IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bo);
        oo.writeObject(operation);
        byte[] bytes = bo.toByteArray();
        bo.close();
        oo.close();
        return bytes;
    }
}
