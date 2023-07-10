package comp7705.chunkserver.service;

import com.alipay.sofa.jraft.error.RemotingException;
import comp7705.chunkserver.server.ChunkServer;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.thread.ServiceThread;

import java.util.concurrent.TimeoutException;

@Slf4j
public class ClearChunkService extends ServiceThread {

    private final ChunkServer chunkServer;


    public ClearChunkService(ChunkServer chunkServer) {
        this.chunkServer = chunkServer;
    }

    @Override
    public String getServiceName() {
        return ClearChunkService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started.", this.getServiceName());
        long lastFlushTime = System.currentTimeMillis();
        while (!this.stopped) {
            if (System.currentTimeMillis() - lastFlushTime > 5000) {
                try {
                    chunkServer.startHeartbeat();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                } catch (RemotingException e) {
                    throw new RuntimeException(e);
                }
                lastFlushTime = System.currentTimeMillis();
            }
        }
        log.info("{} service end.", this.getServiceName());
    }

}
