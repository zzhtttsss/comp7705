package comp7705.chunkserver.test;

import comp7705.chunkserver.common.Const;
import comp7705.chunkserver.interceptor.MetadataInterceptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.protocol.definition.*;
import org.comp7705.protocol.service.MasterServiceGrpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author Reuze
 * @Date 21/05/2023
 */

@Slf4j
public class Master {

    private Server server;
    private int port;

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    @Before
    public void init() throws Exception{
        port = 20051;

        server = ServerBuilder.forPort(port)
                .intercept(new MetadataInterceptor())
                .addService(new MasterHandler())
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    Master.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });

    }

    @After
    public void stopServer() throws InterruptedException {
        stop();
    }

    @Test
    public void start() throws IOException, InterruptedException {
        this.server.start();
        System.out.println("Master started, listening on " + port);
        Master.this.blockUntilShutdown();

    }

    private static class MasterHandler extends MasterServiceGrpc.MasterServiceImplBase {

        @Override
        public void register(DNRegisterRequest registerRequest, StreamObserver<DNRegisterResponse> registerResponse) {
            System.out.println("ChunkIds: " + registerRequest.getChunkIdsList());
            System.out.println("FullCapacity: " + registerRequest.getFullCapacity());
            System.out.println("UsedCapacity: " + registerRequest.getUsedCapacity());

            DNRegisterResponse response = DNRegisterResponse.newBuilder().setId("DNRegisterResponseId")
                    .setPendingCount(1)
                    .build();
            registerResponse.onNext(response);
            registerResponse.onCompleted();
        }

        @Override
        public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseStreamObserver) {
            System.out.println("Chunkserver id: " + request.getId());
            System.out.println("ChunkId: " + request.getChunkIdList());
            System.out.println("IOLoad: " + request.getIOLoad());
            System.out.println("FullCapacity: " + request.getFullCapacity());
            System.out.println("UsedCapacity: " + request.getUsedCapacity());
            System.out.println("SuccessChunkInfos: " + request.getSuccessChunkInfosList());
            System.out.println("FailChunkInfos: " + request.getFailChunkInfosList());
            System.out.println("Invalid Chunks: " + request.getInvalidChunksList());
            System.out.println("IsReady: " + request.getIsReady());

            log.info("Receive heartbeat from " + request.getId());
            log.info(request.toString());
            ChunkInfo chunkInfo = ChunkInfo.newBuilder().setChunkId("1_1")
                    .setDataNodeId("127.0.0.1:10052")
                    .setSendType(Const.MoveSendType)
                    .build();
            HeartbeatResponse response = HeartbeatResponse.newBuilder().addChunkInfos(chunkInfo)
                    .addDataNodeAddress("127.0.0.1:10051")
                    .build();

            responseStreamObserver.onNext(response);
            responseStreamObserver.onCompleted();
            log.info("Send heartbeat to " + request.getId());
            log.info(response.toString());
        }
    }

}
