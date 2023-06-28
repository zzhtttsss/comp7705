package comp7705.chunkserver.server;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import comp7705.chunkserver.client.GrpcClient;
import comp7705.chunkserver.common.Const;
import comp7705.chunkserver.entity.*;
import comp7705.chunkserver.handler.FileHandler;
import comp7705.chunkserver.interceptor.InterceptorConst;
import comp7705.chunkserver.interceptor.MetadataInterceptor;
import comp7705.chunkserver.registry.Registry;
import comp7705.chunkserver.registry.zookeeper.ZkRegistry;
import comp7705.chunkserver.service.FileService;
import comp7705.chunkserver.service.Impl.FileServiceImpl;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.protocol.definition.*;
import org.comp7705.protocol.service.ChunkserverServiceGrpc;
import org.comp7705.protocol.service.MasterServiceGrpc;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    private Registry registry;

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

        this.registry = new ZkRegistry();
        while (true) {
            URL url = registry.lookup("master");
            if (url != null) {
                this.masterHost = url.getIp();
                this.masterPort = url.getPort();
                break;
            }
        }

//        this.masterHost = "127.0.0.1";
//        this.masterPort = 20051;

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
                    executor.execute(new ConsumeSingleChunk(pendingChunkBlockingQueue.poll()));
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
        register();
        consumeSendingTasksThread.start();
        removeChunkThread.start();
        resetHeartbeatTimer();

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


    private void resetHeartbeatTimer() {
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        heartbeatScheduledFuture = scheduledExecutorService.scheduleAtFixedRate(this::startHeartbeat, 0,
                5, TimeUnit.SECONDS);
    }

    private void register() {

        List<String> chunkIds = loadChunk();
        long fullCapacity = fileService.getFullCapacity();
        long usedCapacity = fileService.getUsedCapacity();

        DNRegisterRequest.Builder requestBuilder = DNRegisterRequest.newBuilder();
        requestBuilder.addAllChunkIds(chunkIds);
        DNRegisterRequest request = requestBuilder.setFullCapacity(fullCapacity)
                .setUsedCapacity(usedCapacity)
                .build();

        // todo: handle response
        CountDownLatch countDownLatch = new CountDownLatch(1);
        StreamObserver<DNRegisterResponse> registerResponseStreamObserver = new StreamObserver<DNRegisterResponse>() {
            @Override
            public void onNext(DNRegisterResponse registerResponse) {
                System.out.println("DNRegisterResponse: " + registerResponse.getId());
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.getMessage());
                try {
                    Thread.sleep(3000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // todo: improve retry to connect to master
                // register();
                // System.out.println("Register retry ... ");
                // countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        };
        MasterServiceGrpc.newStub(this.channel).register(request, registerResponseStreamObserver);
        try {
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<String> loadChunk() {
        File[] files = fileService.getFiles();
        List<String> names = new ArrayList<>();
        for (File file : files) {
            if (file.getName().endsWith("_complete")) {
                String[] nameArray = file.getName().split("_");
                names.add(nameArray[0] + "_" + nameArray[1]);
                Chunk chunk = new Chunk(file.getName(), LocalDateTime.ofInstant(Instant.ofEpochMilli(file.lastModified()), ZoneId.systemDefault()));
                ChunkManager.completeChunk(chunk);
            }
        }
        return names;
    }

    private void startHeartbeat() {
        String id = this.address.getHostAddress();
        List<String> chunkIds = ChunkManager.getAllChunkIds();
        List<ChunkInfo> successChunkInfos = getSendResult(successSendResult);
        List<ChunkInfo> failChunkInfos = getSendResult(failSendResult);
        int ioLoad = 1;
        long fullCapacity = fileService.getFullCapacity();
        long usedCapacity = fileService.getUsedCapacity();
        boolean isReady = true;

        HeartbeatRequest.Builder heartbeatRequestBuilder = HeartbeatRequest.newBuilder();
        heartbeatRequestBuilder.addAllChunkId(chunkIds);
        heartbeatRequestBuilder.addAllSuccessChunkInfos(successChunkInfos);
        heartbeatRequestBuilder.addAllFailChunkInfos(failChunkInfos);

        HeartbeatRequest request = heartbeatRequestBuilder.setId(id)
                .setIOLoad(ioLoad)
                .setFullCapacity(fullCapacity)
                .setUsedCapacity(usedCapacity)
                .setIsReady(true)
                .build();

        ListenableFuture<HeartbeatResponse> responseFuture = this.masterServiceFutureStub.heartbeat(request);
        log.info("Start heartbeat to master ...");
        log.info(request.toString());

        try {
            HeartbeatResponse response = responseFuture.get();

            log.info("Receive heartbeat from master ...");
            log.info(response.toString());

            for (ChunkInfo chunkInfo : response.getChunkInfosList()) {
                PendingChunk pendingChunk = new PendingChunk(chunkInfo.getChunkId(), chunkInfo.getSendType(), chunkInfo.getDataNodeId());
                pendingChunkBlockingQueue.offer(pendingChunk);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    // todo: how to ensure the send result arrive master after remove it from map
    private List<ChunkInfo> getSendResult(Map<PendingChunk, SendResult> sendResultMap) {

        List<ChunkInfo> chunkInfos = new ArrayList<>();

        for (Map.Entry<PendingChunk, SendResult>entry : sendResultMap.entrySet()) {
            PendingChunk pendingChunk = entry.getKey();
            SendResult sendResult = entry.getValue();
            String address = sendResult.getAddress();
            int sendType = sendResult.getSendType();
            ChunkInfo.Builder builder = ChunkInfo.newBuilder();
            builder.setChunkId(pendingChunk.getChunkId())
                    .setDataNodeId(address)
                    .setSendType(sendType);
            chunkInfos.add(builder.build());
            sendResultMap.remove(entry.getKey());
        }
        return chunkInfos;
    }

    private class ConsumeSingleChunk implements Runnable {

        private PendingChunk pendingChunk;

        public ConsumeSingleChunk(PendingChunk pendingChunk) {
            this.pendingChunk = pendingChunk;
        }

        @Override
        public void run() {
            if (pendingChunk.getSendType() == Const.DeleteSendType) {
                removedBlockingQueue.offer(pendingChunk);
                return;
            }

            String[] nextAddress = pendingChunk.getAddress().split(":");
            String ip = nextAddress[0];
            int port = Integer.parseInt(nextAddress[1]);

            String chunkId = pendingChunk.getChunkId();
            int chunkSize = fileService.getChunkSize(chunkId);
            SendResult sendResult = new SendResult(chunkId, pendingChunk.getAddress(), pendingChunk.getSendType());
            final Boolean[] isSuccess = {true};
            List<String> checksums = new ArrayList<>();
            try {
                checksums = fileService.getChecksum(chunkId);
            } catch (Exception e) {
                e.printStackTrace();
            }

            Metadata metadata = new Metadata();
            metadata.put(InterceptorConst.CHUNK_ID, chunkId);
            metadata.put(InterceptorConst.CHUNK_SIZE, String.valueOf(chunkSize));
            metadata.put(InterceptorConst.CHECKSUM, String.join(",", checksums));
            metadata.put(InterceptorConst.ADDRESSES, "");

            Channel channel = GrpcClient.getChannel(ip, port);
            channel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
            ChunkserverServiceGrpc.ChunkserverServiceStub serviceStub = ChunkserverServiceGrpc.newStub(channel);

            CountDownLatch countDownLatch = new CountDownLatch(1);
            StreamObserver<TransferChunkResponse> reply = new StreamObserver<TransferChunkResponse>() {
                @Override
                public void onNext(TransferChunkResponse transferChunkResponse) {
                    log.info("Reply: " + transferChunkResponse.getChunkId());
                    log.info("failAddr: " + transferChunkResponse.getFailAddsList());
                }

                @Override
                public void onError(Throwable throwable) {
                    isSuccess[0] = false;
                    countDownLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    countDownLatch.countDown();
                }
            };

            StreamObserver<PieceOfChunk> requestObserver = serviceStub.transferChunk(reply);
            try {
                byte[] chunk = fileService.readChunk(chunkId);
                int len = chunk.length;
                int ptr = 0;
                byte[] pieceByte;
                while (ptr + Const.PieceSize < len) {
                    pieceByte = Arrays.copyOfRange(chunk, ptr, ptr + Const.PieceSize);
                    PieceOfChunk pieceOfChunk = PieceOfChunk.newBuilder()
                            .setPiece(ByteString.copyFrom(pieceByte))
                            .build();
                    requestObserver.onNext(pieceOfChunk);
                    ptr += Const.PieceSize;
                }
                pieceByte = Arrays.copyOfRange(chunk, ptr, len);
                PieceOfChunk pieceOfChunk = PieceOfChunk.newBuilder()
                        .setPiece(ByteString.copyFrom(pieceByte))
                        .build();
                requestObserver.onNext(pieceOfChunk);
                requestObserver.onCompleted();

                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
                isSuccess[0] = false;
            }

            if (!isSuccess[0]) {
                failSendResult.put(pendingChunk, sendResult);
            } else if (pendingChunk.getSendType() == Const.MoveSendType) {
                removedBlockingQueue.offer(pendingChunk);
            } else {
                successSendResult.put(pendingChunk, sendResult);
            }

        }
    }
}