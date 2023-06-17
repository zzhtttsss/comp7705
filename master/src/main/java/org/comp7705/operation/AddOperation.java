package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import org.comp7705.Master;
import org.comp7705.common.AddStage;
import org.comp7705.common.FileType;
import org.comp7705.entity.ChunkTaskResult;
import org.comp7705.metadata.Chunk;
import org.comp7705.metadata.DataNode;
import org.comp7705.metadata.FileNode;
import org.comp7705.protocol.definition.Callback4AddResponse;
import org.comp7705.protocol.definition.CheckArgs4AddResponse;
import org.comp7705.protocol.definition.GetDataNodes4AddResponse;
import org.comp7705.raft.MasterStateMachine;
import org.comp7705.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.comp7705.Master.MASTER;
import static org.comp7705.constant.Common.CHUNK_ID_DELIMITER;

@Data
public class AddOperation implements Operation {

    private static final Logger logger = LoggerFactory.getLogger(AddOperation.class);

    private static final Master master = MASTER;

    private String id;
    private String path;
    private String fileName;
    private long size;
    private String fileNodeId;
    private int chunkNum;
    private String chunkId;
    private List<ChunkTaskResult> infos;
    private List<String> failChunkIds;
    private AddStage stage;

    public AddOperation(String id, String path, String fileName, long size, String fileNodeId, int chunkNum,
                        String chunkId, List<ChunkTaskResult> infos, List<String> failChunkIds, AddStage stage) {
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
    }

    public AddOperation(String id, String path, String fileName, long size, AddStage stage) {
        this.id = id;
        this.path = path;
        this.fileName = fileName;
        this.size = size;
        this.stage = stage;
    }

    @Override
    public Message apply() throws Exception {

        switch (this.stage) {
            case CHECK_ARGS:
                logger.info("Check args for add operation");
                FileNode fileNode = master.getNamespaceManager().addFileNode(this.path, this.fileName, FileType.FILE,
                        this.size);
                if (fileNode == null) {
                    throw new Exception("Failed to add file node");
                }
                logger.info("File node {} added", fileNode.getId());
                return CheckArgs4AddResponse.newBuilder().setFileNodeId(fileNode.getId())
                        .setChunkNum(fileNode.getChunks().size()).build();
            case GET_DATA_NODES:
                ArrayList<ArrayList<DataNode>> allocateResult =
                        master.getDataNodeManager().batchAllocateDataNodes(this.chunkNum);
                List<GetDataNodes4AddResponse.Array> idArrays = new ArrayList<>();
                List<GetDataNodes4AddResponse.Array> addArrays = new ArrayList<>();
                List<Chunk> chunks = new ArrayList<>();
                for (int i = 0; i < chunkNum; i++) {
                    chunkId = StringUtil.concatString(fileNodeId, CHUNK_ID_DELIMITER, String.valueOf(i));
                    Set<String> dataNodeIdSet = new HashSet<>();
                    List<String> dataNodeIds = new ArrayList<>();
                    List<String> dataNodeAdds = new ArrayList<>();
                    for (DataNode dataNode : allocateResult.get(i)) {
                        dataNodeIdSet.add(dataNode.getId());
                        dataNodeIds.add(dataNode.getId());
                        dataNodeAdds.add(dataNode.getAddress());
                    }
                    Chunk chunk = new Chunk(chunkId, dataNodeIdSet);
                    chunks.add(chunk);
                    idArrays.add(GetDataNodes4AddResponse.Array.newBuilder().addAllItems(dataNodeIds).build());
                    addArrays.add(GetDataNodes4AddResponse.Array.newBuilder().addAllItems(dataNodeAdds).build());
                }
                master.getChunkManager().batchAddChunks(chunks);
                return GetDataNodes4AddResponse.newBuilder().addAllDataNodeIds(idArrays).addAllDataNodeAdds(addArrays).build();
            case APPLY_RESULT:
                if (this.failChunkIds != null && !this.failChunkIds.isEmpty()) {
                    master.getNamespaceManager().eraseFileNode(this.path);
                    master.getChunkManager().batchClearPendingDataNodes(this.failChunkIds);
                }
                master.getChunkManager().batchUpdatePendingDataNodes(this.infos);
                master.getDataNodeManager().batchAddChunks(this.infos);
                return Callback4AddResponse.newBuilder().build();
            default:
                throw new Exception("Unknown stage");
        }
    }


}
