package comp7705.chunkserver.entity;

import com.sun.deploy.util.StringUtils;
import comp7705.chunkserver.client.GrpcClient;
import org.comp7705.util.Util;
import comp7705.chunkserver.exception.ChecksumException;
import org.comp7705.grpc.InterceptorConst;
import comp7705.chunkserver.server.ChunkServer;
import comp7705.chunkserver.service.FileService;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.protocol.definition.PieceOfChunk;
import org.comp7705.protocol.definition.TransferChunkResponse;
import org.comp7705.protocol.service.ChunkserverServiceGrpc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
@Slf4j
public class PieceOfChunkStreamObserver implements StreamObserver<PieceOfChunk> {

    ChunkServer chunkServer;

    BlockingQueue<PieceOfChunk> pieceOfChunksQueue;
    List<String> failAddresses;

    AtomicInteger storeChunkCount;
    CountDownLatch nextStreamLatch;

    String chunkId;
    String addresses;
    String chunkSize;
    String checksums;
    List<String> checksumArray;
    List<String> addressArray;
    Chunk chunk;

    boolean isFinishSending;

    boolean isStoreSuccess;

    FileService fileService;
    StreamObserver<TransferChunkResponse> responseStreamObserver;

    ChunkserverServiceGrpc.ChunkserverServiceStub nextStream;
    StreamObserver<PieceOfChunk> nextRequest;

    // the idx of pieceOfChunk
    int ptr = -1;

    public PieceOfChunkStreamObserver(ChunkServer chunkServer, StreamObserver<TransferChunkResponse>
            responseStreamObserver, FileService fileService) {
        this.chunkServer = chunkServer;
        this.fileService = fileService;
        this.responseStreamObserver = responseStreamObserver;

        this.pieceOfChunksQueue = new LinkedBlockingQueue<>();
        this.failAddresses = new ArrayList<>();

        Context context = Context.current();
        Metadata metadata = InterceptorConst.METADATA.get(context);
        // id_id
        this.chunkId = metadata.get(InterceptorConst.CHUNK_ID);
        this.addresses = metadata.get(InterceptorConst.ADDRESSES);
        this.addressArray = Arrays.asList(addresses.split(","));
        // size = 64MB
        this.chunkSize = metadata.get(InterceptorConst.CHUNK_SIZE);
        this.checksums = metadata.get(InterceptorConst.CHECKSUM);
        this.checksumArray = Arrays.asList(checksums.split(","));
        this.chunk = new Chunk(chunkId);

        this.isStoreSuccess = true;

        this.storeChunkCount = new AtomicInteger(0);
        this.nextStreamLatch = new CountDownLatch(1);

        nextStream = getNextStream(chunkId, addressArray.subList(1, addressArray.size()), chunkSize, checksums);
        StreamObserver<TransferChunkResponse> nextResponse = new StreamObserver<TransferChunkResponse>() {
            @Override
            public void onNext(TransferChunkResponse transferChunkResponse) {
                log.info("Receive nextStream response");
                failAddresses.addAll(transferChunkResponse.getFailAddsList());
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.getMessage());
                failAddresses.addAll(addressArray);
                nextStreamLatch.countDown();
            }

            @Override
            public void onCompleted() {
                log.info("NextStream complete");
                nextStreamLatch.countDown();
            }
        };
        nextRequest = nextStream == null ? null : nextStream.transferChunk(nextResponse);

        Thread storeChunkThread = new Thread(() -> {
            int ans = 0;
            try {
                while (!isFinishSending || !pieceOfChunksQueue.isEmpty()) {
                    if (!pieceOfChunksQueue.isEmpty()) {
                        storeChunkCount.incrementAndGet();
                        PieceOfChunk pieceOfChunk = pieceOfChunksQueue.poll();
//                        log.info("Store piece: " + pieceOfChunk.getPiece());
                        fileService.storeChunk(pieceOfChunk, chunkId, Integer.parseInt(chunkSize), checksumArray.get(ans));
                        storeChunkCount.decrementAndGet();
                        ans++;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                failAddresses.add(System.getProperty("serverAddr"));
                isStoreSuccess = false;
                // responseStreamObserver.onError(e);
            } finally {
                storeChunkCount.decrementAndGet();
                try {
                    fileService.finishStoreChunk(chunkId);
                    if (isStoreSuccess) {
                        log.info(chunkId + " stores successfully");
                        ChunkManager.completeChunk(chunk);
                        fileService.renameChunk(chunkId + "_incomplete", chunkId + "_complete");
                        log.info("rename");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        storeChunkThread.start();

        ChunkManager.addPendingChunk(chunk);

    }

    @Override
    public void onNext(PieceOfChunk pieceOfChunk) {
        ptr++;
        log.info("Receive piece {}", ptr);
        // storeChunk
        if (!Util.checksum(pieceOfChunk, checksumArray.get(ptr))) {
            log.warn("Checksum does not match: chunkId " + chunkId);
            failAddresses.add(System.getProperty("serverAddr"));
            isStoreSuccess = false;
            // todo: wait to review
            responseStreamObserver.onError(new ChecksumException());
        }
        // add to block queue to wait to be consume
//        log.info("BlockQueue offer: " + pieceOfChunk.getPiece());
        pieceOfChunksQueue.offer(pieceOfChunk);

        // getNextStream -> send
        if (nextStream != null) {
            try {
                // countDownLatch.await();
                log.info("Call nextStream: " + addressArray.get(0));
                nextRequest.onNext(pieceOfChunk);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            nextStreamLatch.countDown();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("fail to receive a chunk, {}", throwable.getMessage());
    }

    @Override
    public void onCompleted() {

        isFinishSending = true;
        if (nextStream != null) {
            nextRequest.onCompleted();
        }

        TransferChunkResponse.Builder responseBuilder = TransferChunkResponse.newBuilder();
        responseBuilder.setChunkId(chunkId);

        try {
            // wait for sending to next chunk server
            nextStreamLatch.await();
            // wait for writing to disk
            // storeChunkLatch.await();
            while (storeChunkCount.get() > 0) {

            }

            responseBuilder.addAllFailAdds(failAddresses);
            responseStreamObserver.onNext(responseBuilder.build());
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("Finish sending chunk: {}", chunkId);
        responseStreamObserver.onCompleted();
    }

    private ChunkserverServiceGrpc.ChunkserverServiceStub getNextStream(String chunkId, List<String> addresses,
                                                                        String chunkSize, String checksum) {

        if (addresses == null || addresses.size() == 0 || addresses.get(0).equals("")) {
            return null;
        }

        String[] nextAddress = addresses.get(0).split(":");
        String ip = nextAddress[0];
        int port = Integer.parseInt(nextAddress[1]);

        Metadata metadata = new Metadata();
        metadata.put(InterceptorConst.CHUNK_ID, chunkId);
        metadata.put(InterceptorConst.CHUNK_SIZE, chunkSize);
        metadata.put(InterceptorConst.CHECKSUM, checksum);
        metadata.put(InterceptorConst.ADDRESSES, StringUtils.join(addresses,
                ","));

        Channel channel = GrpcClient.getChannel(ip, port);
        channel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        ChunkserverServiceGrpc.ChunkserverServiceStub serviceStub = ChunkserverServiceGrpc.newStub(channel);

        return serviceStub;

    }

}
