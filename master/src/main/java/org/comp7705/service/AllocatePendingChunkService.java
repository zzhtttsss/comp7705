package org.comp7705.service;

import com.alipay.sofa.jraft.entity.Task;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.common.DataNodeStatus;
import org.comp7705.common.DegradeStage;
import org.comp7705.metadata.DataNode;
import org.comp7705.operation.DegradeOperation;
import org.comp7705.thread.ServiceThread;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.comp7705.raft.MasterRequestProcessor.serializeOperation;

@Slf4j
public class AllocatePendingChunkService extends ServiceThread {
    @Override
    public String getServiceName() {
        return AllocatePendingChunkService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started.", this.getServiceName());

        long lastFlushTime = System.currentTimeMillis();
        while (!this.stopped) {
            if (System.currentTimeMillis() - lastFlushTime > 1000) {


                lastFlushTime = System.currentTimeMillis();
            }
        }
        log.info("{} service end.", this.getServiceName());
    }
}
