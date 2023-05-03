package org.comp7705.operation;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.GetStage;
import org.comp7705.metadata.Chunk;
import org.comp7705.metadata.DataNode;
import org.comp7705.protocol.definition.GetDataNodes4GetResponse;

import java.util.ArrayList;
import java.util.List;

import static org.comp7705.Master.MASTER;

@Data
@Slf4j
public class GetOperation implements Operation {

    private Master master;

    private String id;
    private String path;
    private String fileNodeId;
    private int chunkIndex;
    private String chunkId;
    private GetStage stage;

    public GetOperation(String id, String path, String fileNodeId, int chunkIndex, String chunkId, GetStage stage) {
        this.master = MASTER;
        this.id = id;
        this.path = path;
        this.fileNodeId = fileNodeId;
        this.chunkIndex = chunkIndex;
        this.chunkId = chunkId;
        this.stage = stage;
    }

    @Override
    public Object apply() throws Exception {
        switch (this.stage) {
            case CHECK_ARGS:
                return master.getNamespaceManager().getFileNode(this.path);
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
