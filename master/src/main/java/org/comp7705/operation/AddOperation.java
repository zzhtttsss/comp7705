package org.comp7705.operation;

import lombok.Data;
import org.comp7705.Master;
import org.comp7705.common.AddStage;
import org.comp7705.common.FileType;
import org.comp7705.entity.ChunkTaskResult;
import org.comp7705.metadata.DataNode;
import org.comp7705.metadata.FileNode;
import org.comp7705.protocol.definition.CheckArgs4AddResponse;
import org.comp7705.protocol.definition.GetDataNodes4AddResponse;

import java.util.ArrayList;
import java.util.List;

import static org.comp7705.Master.MASTER;

@Data
public class AddOperation implements Operation {

    private Master master;

    private String id;
    private String path;
    private String fileName;
    private long size;
    private String fileNodeId;
    private int chunkNum;
    private String chunkId;
    private List<ChunkTaskResult> infos;
    private String failChunkIds;
    private AddStage stage;

    public AddOperation(String id, String path, String fileName, long size, String fileNodeId, int chunkNum,
                        String chunkId, List<ChunkTaskResult> infos, String failChunkIds, AddStage stage) {
        this.id = id;
        this.path = path;
        this.fileName = fileName;
        this.size = size;
        this.fileNodeId = fileNodeId;
        this.chunkNum = chunkNum;
        this.chunkId = chunkId;
        this.infos = infos;
        this.failChunkIds = failChunkIds;
        this.stage = stage;
        this.master = MASTER;
    }

    @Override
    public Object apply() throws Exception {

        switch (this.stage) {
            case CHECK_ARGS:
                FileNode fileNode = master.getNamespaceManager().addFileNode(this.path, this.fileName, FileType.FILE,
                        this.size);
                if (fileNode == null) {
                    throw new Exception("Failed to add file node");
                }
                return CheckArgs4AddResponse.newBuilder().setFileNodeId(fileNode.getId())
                        .setChunkNum(fileNode.getChunks().size()).build();
            case GET_DATA_NODES:
                ArrayList<ArrayList<DataNode>> allocateResult =
                        master.getDataNodeManager().batchAllocateDataNodes(this.chunkNum);


//                return GetDataNodes4AddResponse.newBuilder().setDataNodeIds(GetDataNodes4AddResponse.Array.newBuilder().addAllItems());
            case UNLOCK_DIR:
                return null;
            default:
                throw new Exception("Unknown stage");
        }
    }


}
