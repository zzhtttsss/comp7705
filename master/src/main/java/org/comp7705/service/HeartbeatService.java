package org.comp7705.service;

import com.alipay.sofa.jraft.entity.Task;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.MasterServer;
import org.comp7705.common.DataNodeStatus;
import org.comp7705.common.DegradeStage;
import org.comp7705.metadata.DataNode;
import org.comp7705.operation.DegradeOperation;
import org.comp7705.thread.ServiceThread;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.comp7705.raft.MasterRequestProcessor.serializeOperation;

@Slf4j
public class HeartbeatService extends ServiceThread {

    private final MasterServer masterServer;

    private final Master master = Master.MASTER;

    public HeartbeatService(MasterServer masterServer) {
        this.masterServer = masterServer;
    }

    @Override
    public String getServiceName() {
        return HeartbeatService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started.", this.getServiceName());

        long lastFlushTime = System.currentTimeMillis();
        while (!this.stopped) {
            if (System.currentTimeMillis() - lastFlushTime > 1000) {
                for(DataNode dataNode: master.getDataNodeManager().getDataNodeMap().values()) {
                    if (dataNode.getStatus() == DataNodeStatus.ALIVE
                            && System.currentTimeMillis() - dataNode.getLastHeartbeatTime() > 3 * 60 * 1000){
                        DegradeOperation operation = new DegradeOperation(UUID.randomUUID().toString(),
                                dataNode.getId(), DegradeStage.BE_WAITING);
                        Task task = new Task();
                        try {
                            task.setData(ByteBuffer.wrap(serializeOperation(operation)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        task.setDone(status -> {});
                        masterServer.getNode().apply(task);
                        continue;
                    }
                    if (dataNode.getStatus() == DataNodeStatus.UNKNOWN
                            && System.currentTimeMillis() - dataNode.getLastHeartbeatTime() > 10 * 60 * 1000) {
                        DegradeOperation operation = new DegradeOperation(UUID.randomUUID().toString(),
                                dataNode.getId(), DegradeStage.BE_DEAD);
                        Task task = new Task();
                        try {
                            task.setData(ByteBuffer.wrap(serializeOperation(operation)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        task.setDone(status -> {});
                        masterServer.getNode().apply(task);
                    }
                }
                lastFlushTime = System.currentTimeMillis();
            }
        }
        log.info("{} service end.", this.getServiceName());
    }
}
