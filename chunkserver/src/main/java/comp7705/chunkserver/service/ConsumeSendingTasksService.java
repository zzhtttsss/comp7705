package comp7705.chunkserver.service;

import com.google.protobuf.ByteString;
import comp7705.chunkserver.client.GrpcClient;
import org.comp7705.constant.Const;
import comp7705.chunkserver.entity.PendingChunk;
import comp7705.chunkserver.entity.SendResult;
import org.comp7705.grpc.InterceptorConst;
import comp7705.chunkserver.server.ChunkServer;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.protocol.definition.PieceOfChunk;
import org.comp7705.protocol.definition.SendType;
import org.comp7705.protocol.definition.TransferChunkResponse;
import org.comp7705.protocol.service.ChunkserverServiceGrpc;
import org.comp7705.thread.ServiceThread;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Slf4j
public class ConsumeSendingTasksService extends ServiceThread {

    private final ChunkServer chunkServer;
    private static final int HEARTBEAT_TIME_OUT = 30000;


    public ConsumeSendingTasksService(ChunkServer chunkServer) {
        this.chunkServer = chunkServer;
    }

    @Override
    public String getServiceName() {
        return ConsumeSendingTasksService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started.", this.getServiceName());
        long lastFlushTime = System.currentTimeMillis();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8, 5,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        while (!this.stopped) {
            if (!chunkServer.getPendingChunkBlockingQueue().isEmpty()) {
                chunkServer.getSendingTaskCount().incrementAndGet();
                executor.execute(new ConsumeSingleChunk(chunkServer.getPendingChunkBlockingQueue().poll()));
            }
        }
        log.info("{} service end.", this.getServiceName());
    }

    public class ConsumeSingleChunk implements Runnable {

        private PendingChunk pendingChunk;

        public ConsumeSingleChunk(PendingChunk pendingChunk) {
            this.pendingChunk = pendingChunk;
        }

        @Override
        public void run() {
            if (pendingChunk.getSendType() == SendType.DELETE) {
                chunkServer.getRemovedBlockingQueue().offer(pendingChunk);
                return;
            }

            String[] nextAddress = pendingChunk.getAddress().split(":");
            String ip = nextAddress[0];
            int port = Integer.parseInt(nextAddress[1]);

            String chunkId = pendingChunk.getChunkId();
            int chunkSize = chunkServer.getFileService().getChunkSize(chunkId);
            SendResult sendResult = new SendResult(chunkId, pendingChunk.getAddress(), pendingChunk.getSendType());
            final Boolean[] isSuccess = {true};
            List<String> checksums = new ArrayList<>();
            try {
                checksums = chunkServer.getFileService().getChecksum(chunkId);
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
                byte[] chunk = chunkServer.getFileService().readChunk(chunkId);
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
                chunkServer.getFailSendResult().put(pendingChunk, sendResult);
            } else if (pendingChunk.getSendType() == SendType.MOVE) {
                chunkServer.getRemovedBlockingQueue().offer(pendingChunk);
            } else {
                chunkServer.getSuccessSendResult().put(pendingChunk, sendResult);
            }

        }
    }


}
