package comp7705.chunkserver;

import com.alipay.sofa.jraft.error.RemotingException;
import comp7705.chunkserver.server.ChunkServer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ChunkserverStartup {


    public static void main(String[] args) {
        log.info("Chunkserver started.");
//        ChunkServer chunkServer = new ChunkServer(4567);
        ChunkServer chunkServer = new ChunkServer(6789);
//        ChunkServer chunkServer = new ChunkServer(4567);
        try {
            chunkServer.start();
            chunkServer.blockUntilShutdown();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (RemotingException e) {
            throw new RuntimeException(e);
        }
    }
}
