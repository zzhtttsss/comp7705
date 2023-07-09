package comp7705.chunkserver.service;

import comp7705.chunkserver.entity.PendingChunk;
import comp7705.chunkserver.entity.SendResult;
import comp7705.chunkserver.server.ChunkServer;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.thread.ServiceThread;

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
                        chunkServer.getFileService().batchDeleteChunk(pendingChunk.getChunkId());
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
