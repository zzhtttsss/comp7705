package org.comp7705.client.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.comp7705.client.utils.ProtobufUtil;
import org.comp7705.protocol.definition.ChunkInfo4Add;
import org.comp7705.protocol.definition.TransferChunkResponse;

import java.util.ArrayList;
import java.util.List;


@Data
@AllArgsConstructor
public class ChunkAddResult {
    public String chunkId;
    public List<String> successNodes;
    public List<String> failNodes;

    public static ChunkAddResult fromTransferChunkResponse(TransferChunkResponse response, List<String> dataNodeAdds, String chunkId) {
        if (response == null) {
            return new ChunkAddResult(chunkId, new ArrayList<>(), dataNodeAdds);
        }
        List<String> failNodes = ProtobufUtil.byteStrList2StrList(response.getFailAddsList().asByteStringList());
        ArrayList<String> successNodes = new ArrayList<>(dataNodeAdds);
        successNodes.removeAll(failNodes);
        return new ChunkAddResult(response.getChunkId(), successNodes, failNodes);
    }

    public ChunkInfo4Add toChunkInfo4Add(){
        return ChunkInfo4Add.newBuilder()
                .setChunkId(chunkId)
                .addAllFailNode(failNodes)
                .addAllSuccessNode(successNodes)
                .build();
    }

}
