package org.comp7705.client.services;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.client.utils.DebugUtil;
import org.comp7705.constant.Const;
import org.comp7705.grpc.InterceptorConst;
import org.comp7705.protocol.definition.Piece;
import org.comp7705.protocol.definition.PieceOfChunk;
import org.comp7705.protocol.definition.SetupStream2DataNodeRequest;
import org.comp7705.protocol.definition.TransferChunkResponse;
import org.comp7705.protocol.service.ChunkserverServiceGrpc;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ChunkClient {
    private final ConcurrentHashMap<String, ManagedChannel> channelMap;
    private static ChunkClient instance = null;

    private static final int port = 9000;

    private ChunkClient() {
        channelMap = new ConcurrentHashMap<>();
    }

    public ChunkserverServiceGrpc.ChunkserverServiceStub getAddStub(String chunkId, List<String> nodeAddresses,
                                                                    int chunkSize, List<String> checkSums) {
        String nextAddress = nodeAddresses.get(0);
        channelMap.computeIfAbsent(nextAddress, key -> ManagedChannelBuilder.forAddress(nextAddress.split(":")[0],
                        Integer.parseInt(nextAddress.split(":")[1]))
                .usePlaintext()
                .build());
        ManagedChannel channel = channelMap.get(nextAddress);

        Metadata metadata = new Metadata();
        metadata.put(InterceptorConst.CHUNK_ID, chunkId);
        metadata.put(InterceptorConst.CHUNK_SIZE, String.valueOf(chunkSize));
        metadata.put(InterceptorConst.CHECKSUM, String.join(",", checkSums));
        metadata.put(InterceptorConst.ADDRESSES, String.join(",", nodeAddresses));
        Channel streamChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        return ChunkserverServiceGrpc.newStub(streamChannel);
    }

    public ChunkserverServiceGrpc.ChunkserverServiceStub getStub(String nodeAddress) {
        channelMap.computeIfAbsent(nodeAddress, key -> ManagedChannelBuilder.forAddress(nodeAddress.split(":")[0],
                Integer.parseInt(nodeAddress.split(":")[1]))
                .usePlaintext()
                .build());
        ManagedChannel channel = channelMap.get(nodeAddress);
        return ChunkserverServiceGrpc.newStub(channel);
    }

    public List<TransferChunkResponse> addFile(String chunkId, List<String> nodeAddresses, int chunkSize,
                                               List<String> checkSums, byte[] buffer) {
        log.info("addFile: chunkId: {}, nodeAddresses: {}, chunkSize: {}, checkSums: {}",
                chunkId, nodeAddresses, chunkSize, checkSums);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        PieceOfChunk.Builder builder = PieceOfChunk.newBuilder();
        List<PieceOfChunk> pieceOfChunks = new ArrayList<>();
        int index = 0;
        for (int off = 0; off < buffer.length; off += Const.MB) {
            log.info("addFile: off: {}, time : {}", off, index++);
            if (off + Const.MB > buffer.length) {
                builder.setPiece(ByteString.copyFrom(buffer, off, buffer.length - off));
            } else {
                builder.setPiece(ByteString.copyFrom(buffer, off, Const.MB));
            }
            pieceOfChunks.add(builder.build());
        }

        ChunkserverServiceGrpc.ChunkserverServiceStub stub = getAddStub(chunkId, nodeAddresses, chunkSize, checkSums);
        ArrayList<TransferChunkResponse> results = new ArrayList<>();

        StreamObserver<TransferChunkResponse> reply = new StreamObserver<TransferChunkResponse>() {
            @Override
            public void onNext(TransferChunkResponse transferChunkResponse) {
                results.add(transferChunkResponse);
                log.info("addFile: onNext: {}", transferChunkResponse);
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
        index = 0;
        for (PieceOfChunk pieceOfChunk : pieceOfChunks) {
            log.info("addFile: index: {}", index++);
            requestObserver.onNext(pieceOfChunk);
        }
        requestObserver.onCompleted();
        log.info("response: {}", results);

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mFailed to run %s because of %s\033[0m\n", DebugUtil.getMethodName(), e);
        }

        return results;
    }

    public boolean getFile(String chunkId, String nodeAddress, BufferedOutputStream bout) {
        SetupStream2DataNodeRequest.Builder requestBuilder = SetupStream2DataNodeRequest.newBuilder();
        requestBuilder.setChunkId(chunkId);
        SetupStream2DataNodeRequest request = requestBuilder.build();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        final Boolean[] isWritten = {true};
        final int[] index = {0};
        StreamObserver<Piece> response = new StreamObserver<Piece>() {
            @Override
            public void onNext(Piece piece) {
                try {
                    log.info("get a piece {}", index[0]);
                    index[0]++;
                    bout.write(piece.getPiece().toByteArray());
                } catch (IOException e) {
                    log.error(e.toString());
                    isWritten[0] = false;
                    System.out.printf("\033[1;31mFailed to run %s because of %s\033[0m\n", DebugUtil.getMethodName(), e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                isWritten[0] = false;
                countDownLatch.countDown();
                log.error("failed to get file", throwable);
            }

            @Override
            public void onCompleted() {
//                isWritten[0] = false;
                countDownLatch.countDown();
            }
        };

        getStub(nodeAddress).setupStream2DataNode(request, response);

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.warn(e.toString());
            System.out.printf("\033[1;31mFailed to run %s because of %s\033[0m\n", DebugUtil.getMethodName(), e);
            return false;
        }
        return isWritten[0];

    }


    @Synchronized
    public static ChunkClient getInstance() {
        if (instance == null) {
            instance = new ChunkClient();
        }
        return instance;
    }

    @Synchronized
    public void close() {
        for (ManagedChannel channel : channelMap.values()) {
            channel.shutdown();
        }
        instance = null;
    }
}
