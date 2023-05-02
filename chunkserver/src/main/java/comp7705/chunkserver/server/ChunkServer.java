package comp7705.chunkserver.server;

import comp7705.chunkserver.entity.PendingChunk;
import comp7705.chunkserver.entity.SendResult;
import comp7705.chunkserver.handler.FileHandler;
import comp7705.chunkserver.interceptor.MetadataInterceptor;
import comp7705.chunkserver.service.FileService;
import comp7705.chunkserver.service.Impl.FileServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.protocol.service.MasterServiceGrpc;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
@Getter
@Slf4j
public class ChunkServer {

    private Server server;
    private InetAddress address;
    private int port;

    private FileService fileService;

    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture heartbeatScheduledFuture;

    private ManagedChannel channel;
    private MasterServiceGrpc.MasterServiceFutureStub masterServiceFutureStub;
    private String masterHost;
    private int masterPort;

    private Map<PendingChunk, SendResult> successSendResult;
    private Map<PendingChunk, SendResult> failSendResult;

    private BlockingQueue<PendingChunk> pendingChunkBlockingQueue;
    private Thread consumeSendingTasksThread;
    private AtomicInteger sendingTaskCount;

    private BlockingQueue<PendingChunk> removedBlockingQueue;
    private Thread removeChunkThread;

    public ChunkServer(int port) {
        try {
            this.address = InetAddress.getLocalHost();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.port = port;
        System.setProperty("pathPrefix", this.address.getHostAddress() + "_" + this.port);
        System.setProperty("serverAddr", this.address.getHostAddress() + ":" + this.port);

        // lock when read and write
        // <key, value> = <chunkId, SendType>
        this.successSendResult = new ConcurrentHashMap<>();
        this.failSendResult = new ConcurrentHashMap<>();

        // todo: zookeeper or etcd to get address
        this.masterHost = "127.0.0.1";
        this.masterPort = 20051;

        this.fileService = new FileServiceImpl();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);

        this.channel = ManagedChannelBuilder.forAddress(this.masterHost, this.masterPort)
                .usePlaintext()
                .build();
        this.masterServiceFutureStub = MasterServiceGrpc.newFutureStub(this.channel);

        this.removedBlockingQueue = new LinkedBlockingQueue<>();
        this.removeChunkThread = new Thread(() -> {
            while (true) {
                SendResult sendResult;
                if (!removedBlockingQueue.isEmpty()) {
                    PendingChunk pendingChunk = removedBlockingQueue.poll();
                    sendResult = new SendResult(pendingChunk.getChunkId(), pendingChunk.getAddress(), pendingChunk.getSendType());
                    try {
                        fileService.deleteChunk(pendingChunk.getChunkId());
                        successSendResult.put(pendingChunk, sendResult);
                    } catch (Exception e) {
                        e.printStackTrace();
                        failSendResult.put(pendingChunk, sendResult);
                    }
                }
            }
        });

        this.pendingChunkBlockingQueue = new LinkedBlockingQueue<>();
        this.sendingTaskCount = new AtomicInteger(0);
        this.consumeSendingTasksThread = new Thread(() -> {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
            while (true) {
                if (!pendingChunkBlockingQueue.isEmpty()) {
                    sendingTaskCount.incrementAndGet();
                    // todo
                    // executor.execute(new ConsumeSingleChunk(pendingChunkBlockingQueue.poll()));
                }
            }
        });
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .intercept(new MetadataInterceptor())
                .addService(new FileHandler(this, this.fileService))
                .build()
                .start();
        System.out.println("Server started, listening on " + port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    ChunkServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });

        // todo: check register status, only register complete chunk, if fails, retry
        // register();
        // consumeSendingTasksThread.start();
        // removeChunkThread.start();
        // resetHeartbeatTimer();

    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}