package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.GetStage;
import org.comp7705.metadata.Chunk;
import org.comp7705.metadata.DataNode;
import org.comp7705.metadata.FileNode;
import org.comp7705.protocol.definition.CheckArgs4AddResponse;
import org.comp7705.protocol.definition.CheckArgs4GetRequest;
import org.comp7705.protocol.definition.CheckArgs4GetResponse;
import org.comp7705.protocol.definition.GetDataNodes4GetResponse;

import java.util.ArrayList;
import java.util.List;

import static org.comp7705.Master.MASTER;

@Data
@Slf4j
public class GetOperation implements Operation {
    private static final Master master = MASTER;

    private String id;
    private String path;
    private String fileNodeId;
    private int chunkIndex;
    private String chunkId;
    private GetStage stage;

    public GetOperation(String id, String path, GetStage stage) {
        this.id = id;
        this.path = path;
        this.stage = stage;
    }

    public GetOperation(String id, String fileNodeId, int chunkIndex, GetStage stage) {
        this.id = id;
        this.fileNodeId = fileNodeId;
        this.chunkIndex = chunkIndex;
        this.chunkId = fileNodeId + "_" + chunkIndex;
        this.stage = stage;
    }

    @Override
    public Message apply() throws Exception {
        switch (this.stage) {
            case CHECK_ARGS:
                FileNode fileNode = master.getNamespaceManager().getFileNode(this.path);
                return CheckArgs4GetResponse.newBuilder().setFileNodeId(fileNode.getId())
                        .setChunkNum(fileNode.getChunks().size()).setFileSize(fileNode.getSize()).build();
            case GET_DATA_NODES:
                Chunk chunk = master.getChunkManager().getChunkMap().get(this.chunkId);
                if (chunk == null) {
                    throw new Exception("Chunk not found");
                }
                ArrayList<DataNode> dataNodes = master.getDataNodeManager().getSortedDataNodes(chunk.getDataNodes());
                List<String> dataNodeIds = new ArrayList<>();
                List<String> dataNodeAdds = new ArrayList<>();
                for (DataNode dataNode : dataNodes) {
                    dataNodeIds.add(dataNode.getId());
                    dataNodeAdds.add(dataNode.getAddress());
                }
                return GetDataNodes4GetResponse.newBuilder().addAllDataNodeIds(dataNodeIds)
                        .addAllDataNodeAddrs(dataNodeAdds).setChunkIndex(this.chunkIndex).build();
            default:
                throw new Exception("Unknown stage");
        }
    }
}
