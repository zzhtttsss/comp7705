package comp7705.chunkserver.test;

import com.alipay.sofa.jraft.error.RemotingException;
import comp7705.chunkserver.server.ChunkServer;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public class Server {

    @Test
    public void start1() throws IOException, InterruptedException, RemotingException, TimeoutException {
        ChunkServer chunkServer = new ChunkServer();
        chunkServer.start();
        chunkServer.blockUntilShutdown();
    }

    @Test
    public void start2() throws IOException, InterruptedException, RemotingException, TimeoutException {
        ChunkServer chunkServer = new ChunkServer();
        chunkServer.start();
        chunkServer.blockUntilShutdown();
    }

    @Test
    public void start3() throws IOException, InterruptedException, RemotingException, TimeoutException {
        ChunkServer chunkServer = new ChunkServer();
        chunkServer.start();
        chunkServer.blockUntilShutdown();
    }


}
