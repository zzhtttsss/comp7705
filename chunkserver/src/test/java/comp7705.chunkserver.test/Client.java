package comp7705.chunkserver.test;

import com.google.protobuf.ByteString;
import comp7705.chunkserver.client.GrpcClient;
import comp7705.chunkserver.common.Const;
import comp7705.chunkserver.common.Util;
import comp7705.chunkserver.interceptor.InterceptorConst;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.protocol.definition.Piece;
import org.comp7705.protocol.definition.PieceOfChunk;
import org.comp7705.protocol.definition.SetupStream2DataNodeRequest;
import org.comp7705.protocol.definition.TransferChunkResponse;
import org.comp7705.protocol.service.ChunkserverServiceGrpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
@Slf4j
public class Client {

    private ManagedChannel channel;
    private ChunkserverServiceGrpc.ChunkserverServiceStub stub;
    private String ip = "127.0.0.1";
    private int port = 10051;
    private byte[] fileData;
    private String chunkId;
    private String chunkSize;
    private String addresses;
    private String checksum;

    @Before
    public void init() {
        this.channel = ManagedChannelBuilder.forAddress(ip, port)
                .usePlaintext()
                .build();
        this.stub = ChunkserverServiceGrpc.newStub(this.channel);
        this.fileData = "File Data".getBytes();

        this.chunkId = "2_1";
        this.chunkSize = String.valueOf(fileData.length);
        this.addresses = "127.0.0.1:10052,127.0.0.1:10053";

        this.checksum = Util.getChecksum(this.fileData);

    }

    @After
    public void destroy() {
        channel.shutdown();
    }

    @Test
    public void testTransferChunk() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Metadata metadata = new Metadata();
        metadata.put(InterceptorConst.CHUNK_ID, this.chunkId);
        metadata.put(InterceptorConst.CHUNK_SIZE, this.chunkSize);
        metadata.put(InterceptorConst.CHECKSUM, this.checksum);
        metadata.put(InterceptorConst.ADDRESSES, this.addresses);

        Channel channel = ClientInterceptors.intercept(this.channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        this.stub = ChunkserverServiceGrpc.newStub(channel);

        StreamObserver<TransferChunkResponse> reply = new StreamObserver<TransferChunkResponse>() {
            @Override
            public void onNext(TransferChunkResponse transferChunkResponse) {
                log.info("Reply: " + transferChunkResponse.getChunkId());
            }

            @Override
            public void onError(Throwable throwable) {
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        };

        StreamObserver<PieceOfChunk> requestObserver = stub.transferChunk(reply);

        PieceOfChunk pieceOfChunk = PieceOfChunk.newBuilder().
                setPiece(ByteString.copyFrom(this.fileData)).build();
        requestObserver.onNext(pieceOfChunk);
        requestObserver.onNext(pieceOfChunk);
        requestObserver.onCompleted();

        countDownLatch.await();
        log.info("Client send finished ...");

    }

    @Test
    public void testMultiTransferChunk() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        String data = "heartbeat";
        String chunkId = "1_1";

        PieceOfChunk.Builder builder = PieceOfChunk.newBuilder();
        List<PieceOfChunk> pieceOfChunks = new ArrayList<>();
        List<String> checksumArray = new ArrayList<>();

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            stringBuilder.append(data + i);
        }

        byte[] input = stringBuilder.toString().getBytes();
        for (int i = 0; i < input.length; i += Const.PieceSize) {
            byte[] piece;
            if (i + Const.PieceSize < input.length) {
                piece = Arrays.copyOfRange(input, i, i + Const.PieceSize);
            } else {
                piece = Arrays.copyOfRange(input, i, input.length);
            }
            checksumArray.add(Util.getChecksum(piece));
            builder.setPiece(ByteString.copyFrom(piece));
            pieceOfChunks.add(builder.build());
        }

        Metadata metadata = new Metadata();
        metadata.put(InterceptorConst.CHUNK_ID, chunkId);
        metadata.put(InterceptorConst.CHUNK_SIZE, this.chunkSize);
        metadata.put(InterceptorConst.CHECKSUM, String.join(",", checksumArray));
        metadata.put(InterceptorConst.ADDRESSES, this.addresses);

        Channel channel = ClientInterceptors.intercept(this.channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        this.stub = ChunkserverServiceGrpc.newStub(channel);

        StreamObserver<TransferChunkResponse> reply = new StreamObserver<TransferChunkResponse>() {
            @Override
            public void onNext(TransferChunkResponse transferChunkResponse) {
                log.info("Reply: " + transferChunkResponse.getChunkId());
                log.info("failAddr: " + transferChunkResponse.getFailAddsList());
            }

            @Override
            public void onError(Throwable throwable) {
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        };

        StreamObserver<PieceOfChunk> requestObserver = stub.transferChunk(reply);

        for (PieceOfChunk pieceOfChunk : pieceOfChunks) {
            requestObserver.onNext(pieceOfChunk);
        }
        requestObserver.onCompleted();

        countDownLatch.await();
        log.info("Client send finished ...");

    }

    @Test
    public void testSetupStream2DataNode() throws Exception {

        SetupStream2DataNodeRequest.Builder requestBuilder = SetupStream2DataNodeRequest.newBuilder();
        requestBuilder.setChunkId("1_1");
        SetupStream2DataNodeRequest request = requestBuilder.build();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<Piece> pieces = new ArrayList<>();
        StreamObserver<Piece> response = new StreamObserver<Piece>() {
            @Override
            public void onNext(Piece piece) {
                pieces.add(piece);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        };

        this.stub.setupStream2DataNode(request, response);

        countDownLatch.await();
        for (Piece piece : pieces) {
            System.out.println(piece.getPiece());
        }

    }

    @Test
    public void offlineTest() throws InterruptedException {

        String input = "heartbeat";
        String chunkId = "1_2";

        PieceOfChunk.Builder builder = PieceOfChunk.newBuilder();
        List<PieceOfChunk> pieceOfChunks = new ArrayList<>();
        List<String> checksumArray = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String piece = input + i;
            checksumArray.add(Util.getChecksum(piece));
            builder.setPiece(ByteString.copyFrom((piece).getBytes()));
            pieceOfChunks.add(builder.build());
        }

        Metadata metadata = new Metadata();
        metadata.put(InterceptorConst.CHUNK_ID, chunkId);
        metadata.put(InterceptorConst.CHUNK_SIZE, chunkSize);
        metadata.put(InterceptorConst.CHECKSUM, checksum);

        Channel channel = GrpcClient.getChannel(ip, 10052);
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
                System.out.println(throwable.getMessage());
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        };

        StreamObserver<PieceOfChunk> requestObserver = serviceStub.transferChunk(reply);

        for (PieceOfChunk pieceOfChunk : pieceOfChunks) {
            requestObserver.onNext(pieceOfChunk);
        }
        requestObserver.onCompleted();

        countDownLatch.await();
        log.info("Client send finished ...");
    }

}

