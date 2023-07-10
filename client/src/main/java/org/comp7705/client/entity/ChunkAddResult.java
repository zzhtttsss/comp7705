package org.comp7705.client.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.comp7705.client.utils.ProtobufUtil;
import org.comp7705.protocol.definition.ChunkInfo4Add;
import org.comp7705.protocol.definition.TransferChunkResponse;

import java.util.ArrayList;
import java.util.List;


@Data
public class ChunkAddResult {
    public String chunkId;
    public List<String> successNodes;
    public List<String> failNodes;

//    public static ChunkAddResult fromTransferChunkResponse(List<String> failNodes, List<String> dataNodeIds,
//                                                           String chunkId) {
////        if (response == null) {
////            return new ChunkAddResult(chunkId, new ArrayList<>(), dataNodeAdds);
////        }
//        ArrayList<String> successNodes = new ArrayList<>(dataNodeIds);
//        successNodes.removeAll(failNodes);
//        return new ChunkAddResult(chunkId, successNodes, failNodes);
//    }


    public ChunkAddResult(String chunkId, List<String> successNodes, List<String> failNodes) {
        this.chunkId = chunkId;
        this.successNodes = successNodes;
        this.failNodes = failNodes;
    }

    public ChunkInfo4Add toChunkInfo4Add(){
        return ChunkInfo4Add.newBuilder()
                .setChunkId(chunkId)
                .addAllFailNode(failNodes)
                .addAllSuccessNode(successNodes)
                .build();
    }

}
