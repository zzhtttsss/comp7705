package comp7705.chunkserver.service;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RemotingException;
import comp7705.chunkserver.entity.PendingChunk;
import comp7705.chunkserver.entity.SendResult;
import comp7705.chunkserver.server.ChunkServer;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.protocol.definition.HeartbeatRequest;
import org.comp7705.protocol.definition.HeartbeatResponse;
import org.comp7705.protocol.definition.MkdirRequest;
import org.comp7705.thread.ServiceThread;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Slf4j
public class HeartbeatService extends ServiceThread {

    private final ChunkServer chunkServer;


    public HeartbeatService(ChunkServer chunkServer) {
        this.chunkServer = chunkServer;
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
            if (System.currentTimeMillis() - lastFlushTime > 5000) {
                SendResult sendResult;
                if (!chunkServer.getRemovedBlockingQueue().isEmpty()) {
                    PendingChunk pendingChunk = chunkServer.getRemovedBlockingQueue().poll();
                    sendResult = new SendResult(pendingChunk.getChunkId(), pendingChunk.getAddress(),
                            pendingChunk.getSendType());
                    try {
                        chunkServer.getFileService().deleteChunk(pendingChunk.getChunkId());
                        chunkServer.getSuccessSendResult().put(pendingChunk, sendResult);
                    } catch (Exception e) {
                        e.printStackTrace();
                        chunkServer.getFailSendResult().put(pendingChunk, sendResult);
                    }
                }
            }
        }
        log.info("{} service end.", this.getServiceName());
    }

}
