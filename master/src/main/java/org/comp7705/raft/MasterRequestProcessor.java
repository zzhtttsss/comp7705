package org.comp7705.raft;

import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.util.Requires;
import lombok.Getter;
import org.comp7705.Master;
import org.comp7705.MasterServer;
import org.comp7705.common.AddStage;
import org.comp7705.common.GetStage;
import org.comp7705.common.RequestType;
import org.comp7705.entity.ChunkTaskResult;
import org.comp7705.metadata.DataNode;
import org.comp7705.operation.*;
import org.comp7705.protocol.definition.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

public class MasterRequestProcessor<T> implements RpcProcessor<T> {

    private static final Logger logger = LoggerFactory.getLogger(MasterRequestProcessor.class);


    @Getter
    private final Class<T> reqClazz;

    private final MasterServer masterServer;

    private final Master master = Master.MASTER;

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
            case CHECK_ARGS_4_ADD:
                CheckArgs4AddRequest checkArgs4AddRequest = (CheckArgs4AddRequest) request;
                operation = new AddOperation(UUID.randomUUID().toString(), checkArgs4AddRequest.getPath(),
                        checkArgs4AddRequest.getFileName(),checkArgs4AddRequest.getSize(), AddStage.CHECK_ARGS);
                break;
            case GET_DATA_NODES_4_ADD:
                GetDataNodes4AddRequest getDataNodes4AddRequest = (GetDataNodes4AddRequest) request;
                operation = new AddOperation(UUID.randomUUID().toString(), getDataNodes4AddRequest.getFileNodeId(),
                        getDataNodes4AddRequest.getChunkNum(), AddStage.GET_DATA_NODES);
                break;
            case CALLBACK_4_ADD:
                Callback4AddRequest callback4AddRequest = (Callback4AddRequest) request;
                operation = new AddOperation(UUID.randomUUID().toString(), callback4AddRequest.getFileNodeId(),
                        callback4AddRequest.getFilePath(), callback4AddRequest.getInfosList()
                        .stream()
                        .map(e -> new ChunkTaskResult(e.getChunkId(), new ArrayList<>(e.getFailNodeList()),
                                new ArrayList<>(e.getSuccessNodeList()), 0))
                        .collect(Collectors.toList()), new ArrayList<>(callback4AddRequest.getFailChunkIdsList()),
                        AddStage.APPLY_RESULT);
                break;
            case CHECK_ARGS_4_GET:
                CheckArgs4GetRequest checkArgs4GetRequest = (CheckArgs4GetRequest) request;
                operation = new GetOperation(UUID.randomUUID().toString(), checkArgs4GetRequest.getPath(),
                        GetStage.CHECK_ARGS);
                break;
            case GET_DATA_NODES_4_GET:
                GetDataNodes4GetRequest getDataNodes4GetRequest = (GetDataNodes4GetRequest) request;
                operation = new GetOperation(UUID.randomUUID().toString(), getDataNodes4GetRequest.getFileNodeId(),
                        getDataNodes4GetRequest.getChunkIndex(), GetStage.GET_DATA_NODES);
                break;
            case LIST:
                ListRequest listRequest = (ListRequest) request;
                operation = new ListOperation(UUID.randomUUID().toString(), listRequest.getPath());
                break;
            case MKDIR:
                MkdirRequest mkDirRequest = (MkdirRequest) request;
                operation = new MkdirOperation(UUID.randomUUID().toString(), mkDirRequest.getPath(),
                        mkDirRequest.getDirName());
                break;
            case MOVE:
                MoveRequest moveRequest = (MoveRequest) request;
                operation = new MoveOperation(UUID.randomUUID().toString(), moveRequest.getSourcePath(),
                        moveRequest.getTargetPath());
                break;
            case REMOVE:
                RemoveRequest removeRequest = (RemoveRequest) request;
                operation = new RemoveOperation(UUID.randomUUID().toString(), removeRequest.getPath());
                break;
            case RENAME:
                RenameRequest renameRequest = (RenameRequest) request;
                operation = new RenameOperation(UUID.randomUUID().toString(), renameRequest.getPath(),
                        renameRequest.getNewName());
                break;
            case STAT:
                StatRequest statRequest = (StatRequest) request;
                operation = new StatOperation(UUID.randomUUID().toString(), statRequest.getPath());
                break;
            case HEARTBEAT:
                HeartbeatRequest heartbeatRequest = (HeartbeatRequest) request;
                operation = new HeartbeatOperation(UUID.randomUUID().toString(), heartbeatRequest.getId(),
                        new ArrayList<>(heartbeatRequest.getChunkIdList()), heartbeatRequest.getIOLoad(),
                        heartbeatRequest.getFullCapacity(), heartbeatRequest.getUsedCapacity(),
                        conv2ChunkSendInfo(heartbeatRequest.getSuccessChunkInfosList()),
                        conv2ChunkSendInfo(heartbeatRequest.getFailChunkInfosList()),
                        new ArrayList<>(heartbeatRequest.getInvalidChunksList()), heartbeatRequest.getIsReady());
                break;
            case REGISTER:
                DNRegisterRequest registerRequest = (DNRegisterRequest) request;
                operation = new RegisterOperation(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                        rpcCtx.getRemoteAddress().substring(1).split(":")[0] + ":" + registerRequest.getPort(),
                        new ArrayList<>(registerRequest.getChunkIdsList()), registerRequest.getFullCapacity(),
                        registerRequest.getUsedCapacity(), master.getDataNodeManager()
                        .isNeed2Expand(registerRequest.getUsedCapacity(), registerRequest.getFullCapacity()));
                logger.info("Register operation: {}", operation);
                break;
            default:
                return;
        }
        closure = new RequestProcessClosure(operation, rpcCtx);
        try {
            task.setData(ByteBuffer.wrap(serializeOperation(operation)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        task.setDone(closure);
        masterServer.getNode().apply(task);
    }

    @Override
    public String interest() {
        return this.reqClazz.getName();
    }

    public static byte[] serializeOperation(Operation operation) throws IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bo);
        oo.writeObject(operation);
        byte[] bytes = bo.toByteArray();
        bo.close();
        oo.close();
        return bytes;
    }

    private List<DataNode.ChunkSendInfo> conv2ChunkSendInfo(List<ChunkInfo> chunkInfos) {
        List<DataNode.ChunkSendInfo> chunkSendInfos = new ArrayList<>(chunkInfos.size());
        for (ChunkInfo chunkInfo: chunkInfos) {
            chunkSendInfos.add(new DataNode.ChunkSendInfo(chunkInfo.getDataNodeId(), chunkInfo.getChunkId(),
                    chunkInfo.getSendType()));
        }
        return chunkSendInfos;
    }
}
