package comp7705.chunkserver.server;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import comp7705.chunkserver.entity.*;
import comp7705.chunkserver.handler.FileHandler;
import comp7705.chunkserver.interceptor.MetadataInterceptor;
import comp7705.chunkserver.registry.Registry;
import comp7705.chunkserver.service.ClearChunkService;
import comp7705.chunkserver.service.ConsumeSendingTasksService;
import comp7705.chunkserver.service.FileService;
import comp7705.chunkserver.service.HeartbeatService;
import comp7705.chunkserver.service.Impl.FileServiceImpl;
import io.grpc.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.grpc.MasterGrpcHelper;
import org.comp7705.protocol.definition.*;
import org.comp7705.protocol.service.MasterServiceGrpc;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
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
    private static final int TIME_OUT = 50000;

    private DataNode dataNode;



    private String groupId = "master";
    private String masterAddress = "localhost";
    private int masterLeaderPort = 8081;
    private final CliClientServiceImpl cliClientService;

    private final FileService fileService;

    private final ScheduledExecutorService scheduledExecutorService;

    private ManagedChannel channel;
    private MasterServiceGrpc.MasterServiceFutureStub masterServiceFutureStub;
    private String masterHost;
    private int masterPort;

    private final Map<PendingChunk, SendResult> successSendResult;
    private final Map<PendingChunk, SendResult> failSendResult;

    private final BlockingQueue<PendingChunk> pendingChunkBlockingQueue;
    private final AtomicInteger sendingTaskCount;

    private final BlockingQueue<PendingChunk> removedBlockingQueue;

    private Registry registry;

    private final HeartbeatService heartbeatService;

    private final ConsumeSendingTasksService consumeSendingTasksService;

    private final ClearChunkService clearChunkService;

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

//        this.registry = new ZkRegistry();
//        while (true) {
//            URL url = registry.lookup("master");
//            if (url != null) {
//                this.masterHost = url.getIp();
//                this.masterPort = url.getPort();
//                break;
//            }
//        }

        final String groupId = "master";
        final String confStr = "127.0.0.1:8081";
        MasterGrpcHelper.initGRpc();

        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());

        try {
            if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
                throw new IllegalStateException("Refresh leader failed");
            }
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

//        this.masterHost = "127.0.0.1";
//        this.masterPort = 20051;

        this.fileService = new FileServiceImpl();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);

//        this.channel = ManagedChannelBuilder.forAddress(this.masterHost, this.masterPort)
//                .usePlaintext()
//                .build();
//        this.masterServiceFutureStub = MasterServiceGrpc.newFutureStub(this.channel);

        this.removedBlockingQueue = new LinkedBlockingQueue<>();

        this.pendingChunkBlockingQueue = new LinkedBlockingQueue<>();
        this.sendingTaskCount = new AtomicInteger(0);

        this.heartbeatService = new HeartbeatService(this);
        this.consumeSendingTasksService = new ConsumeSendingTasksService(this);
        this.clearChunkService = new ClearChunkService(this);
    }

    public void start() throws IOException, InterruptedException, TimeoutException, RemotingException {
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
        heartbeatService.start();
        consumeSendingTasksService.start();
        clearChunkService.start();
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



    private void register() throws InterruptedException, TimeoutException, RemotingException {

        List<String> chunkIds = loadChunk();
        long fullCapacity = fileService.getFullCapacity();
        long usedCapacity = fileService.getUsedCapacity();

        DNRegisterRequest.Builder requestBuilder = DNRegisterRequest.newBuilder();
        requestBuilder.addAllChunkIds(chunkIds);
        DNRegisterRequest request = requestBuilder
                .setFullCapacity(fullCapacity)
                .setUsedCapacity(usedCapacity)
                .setPort(this.port)
                .build();

        PeerId leader = refreshAndGetLeader();
        DNRegisterResponse response = (DNRegisterResponse) cliClientService.getRpcClient()
                .invokeSync(leader.getEndpoint(), request, TIME_OUT);
        this.dataNode = new DataNode(response.getId(), response.getPendingCount(),
                response.getPendingCount() == 0, 0);
    }

    private List<String> loadChunk() {
        File[] files = fileService.getFiles();
        List<String> names = new ArrayList<>();
        for (File file : files) {
            if (file.getName().endsWith("_complete")) {
                String[] nameArray = file.getName().split("_");
                names.add(nameArray[0] + "_" + nameArray[1]);
                Chunk chunk = new Chunk(file.getName(), file.lastModified());
                ChunkManager.completeChunk(chunk);
            }
        }
        return names;
    }

    public void startHeartbeat() throws InterruptedException, TimeoutException, RemotingException {
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

        HeartbeatRequest request = heartbeatRequestBuilder.setId(dataNode.getId())
                .setIOLoad(ioLoad)
                .setFullCapacity(fullCapacity)
                .setUsedCapacity(usedCapacity)
                .setIsReady(true)
                .build();

        PeerId leader = refreshAndGetLeader();
        log.info("Start heartbeat to master ...");
        log.info(request.toString());
        HeartbeatResponse response = (HeartbeatResponse) cliClientService.getRpcClient()
                .invokeSync(leader.getEndpoint(), request, TIME_OUT);

        try {
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
            SendType sendType = sendResult.getSendType();
            ChunkInfo.Builder builder = ChunkInfo.newBuilder();
            builder.setChunkId(pendingChunk.getChunkId())
                    .setDataNodeId(address)
                    .setSendType(sendType);
            chunkInfos.add(builder.build());
            sendResultMap.remove(entry.getKey());
        }
        return chunkInfos;
    }


    public PeerId refreshAndGetLeader() throws InterruptedException, TimeoutException {
        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 5000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }
        return RouteTable.getInstance().selectLeader(groupId);

    }
}