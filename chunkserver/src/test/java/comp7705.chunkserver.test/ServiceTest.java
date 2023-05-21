package comp7705.chunkserver.test;

/**
 * @author Reuze
 * @Date 22/04/2023
 */
import comp7705.chunkserver.entity.URL;
import comp7705.chunkserver.registry.Registry;
import comp7705.chunkserver.registry.zookeeper.ZkRegistry;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class ServiceTest {

    @Test
    public void helloTest() {


    }

    @Test
    public void zkRegisterTest() throws InterruptedException {

        AtomicInteger waitChunkserver = new AtomicInteger(0);
        AtomicInteger waitMaster = new AtomicInteger(0);

        Thread master = new Thread(() -> {
            Registry registry = new ZkRegistry();
            URL url = new URL();
            url.setIp("23.23.23.23");
            url.setPort(2181);
            url.setPath("master");
            registry.register(url);
            waitMaster.set(1);
            while (waitChunkserver.get() == 0) {

            }
        });

        Thread chunkserver = new Thread(() -> {
            while (waitMaster.get() == 0) {

            }
            Registry registry = new ZkRegistry();
            System.out.println("Result: " + registry.lookup("master"));
            waitChunkserver.set(1);
        });

        master.start();
        chunkserver.start();

        chunkserver.join();
        master.join();

    }

}
