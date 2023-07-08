package comp7705.chunkserver.handler;

import com.google.protobuf.ByteString;
import comp7705.chunkserver.entity.PieceOfChunkStreamObserver;
import comp7705.chunkserver.server.ChunkServer;
import comp7705.chunkserver.service.FileService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.protocol.definition.Piece;
import org.comp7705.protocol.definition.PieceOfChunk;
import org.comp7705.protocol.definition.SetupStream2DataNodeRequest;
import org.comp7705.protocol.definition.TransferChunkResponse;
import org.comp7705.protocol.service.ChunkserverServiceGrpc;

import java.util.Arrays;

import static org.comp7705.constant.Const.PieceSize;

/**
 * @author Reuze
 * @Date 22/04/2023
 */
@Slf4j
public class FileHandler extends ChunkserverServiceGrpc.ChunkserverServiceImplBase {

    private ChunkServer chunkServer;
    private FileService fileService;

    public FileHandler(ChunkServer chunkServer, FileService fileService) {
        this.chunkServer = chunkServer;
        this.fileService = fileService;
    }

    @Override
    public StreamObserver<PieceOfChunk> transferChunk(StreamObserver<TransferChunkResponse> responseStreamObserver) {
        return new PieceOfChunkStreamObserver(chunkServer, responseStreamObserver, fileService);
    }

    @Override
    public void setupStream2DataNode(SetupStream2DataNodeRequest args, StreamObserver<Piece> response) {

        String chunkId = args.getChunkId();
        byte[] chunkContent = new byte[0];
        try {
            chunkContent = fileService.readChunk(chunkId);
            int len = chunkContent.length;
            int ptr = 0;
            byte[] pieceByte;
            while (ptr + PieceSize < len) {
                pieceByte = Arrays.copyOfRange(chunkContent, ptr, ptr + PieceSize);
                Piece piece = Piece.newBuilder()
                        .setPiece(ByteString.copyFrom(pieceByte))
                        .build();
                response.onNext(piece);
                ptr += PieceSize;
            }
            pieceByte = Arrays.copyOfRange(chunkContent, ptr, len);
            Piece piece = Piece.newBuilder()
                    .setPiece(ByteString.copyFrom(pieceByte))
                    .build();
            response.onNext(piece);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            response.onCompleted();
        }
    }
}
