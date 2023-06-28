package comp7705.chunkserver.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public class GrpcClient {

    private static Map<String, ManagedChannel> cache = new ConcurrentHashMap<>();

    public static ManagedChannel getChannel(String ip, int port) {

        if (cache.containsKey(ip + port)) {
            return cache.get(ip + port);
        }

        ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, port)
                .usePlaintext()
                .build();
        cache.put(ip + port, channel);
        return channel;
    }

    public static boolean shutdown(String ip, int port) {

        if (cache.containsKey(ip + port)) {
            ManagedChannel channel = cache.remove(ip + port);
            channel.shutdown();
            return true;
        }

        return false;
    }

}
