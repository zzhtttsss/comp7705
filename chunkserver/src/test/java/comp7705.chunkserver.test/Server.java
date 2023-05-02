package comp7705.chunkserver.test;

import comp7705.chunkserver.server.ChunkServer;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public class Server {

    @Test
    public void start1() throws IOException, InterruptedException {
        ChunkServer chunkServer = new ChunkServer(10051);
        chunkServer.start();
        chunkServer.blockUntilShutdown();
    }

    @Test
    public void start2() throws IOException, InterruptedException {
        ChunkServer chunkServer = new ChunkServer(10052);
        chunkServer.start();
        chunkServer.blockUntilShutdown();
    }

    @Test
    public void start3() throws IOException, InterruptedException {
        ChunkServer chunkServer = new ChunkServer(10053);
        chunkServer.start();
        chunkServer.blockUntilShutdown();
    }


}
