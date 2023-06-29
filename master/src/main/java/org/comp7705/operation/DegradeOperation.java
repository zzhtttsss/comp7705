package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import org.comp7705.Master;
import org.comp7705.common.DataNodeStatus;
import org.comp7705.common.DegradeStage;
import org.comp7705.metadata.DataNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.comp7705.Master.MASTER;
@Data
public class DegradeOperation implements Operation {

    private static final Logger logger = LoggerFactory.getLogger(DegradeOperation.class);

    private static final Master master = MASTER;

    private String id;
    private String dataNodeId;
    private DegradeStage stage;

    public DegradeOperation(String id, String dataNodeId, DegradeStage stage) {
        this.id = id;
        this.dataNodeId = dataNodeId;
        this.stage = stage;
    }

    @Override
    public Message apply() throws Exception {


        DataNode dataNode = master.getDataNodeManager().getDataNodeMap().get(dataNodeId);
        if (dataNode == null) {
            return null;
        }

        switch (stage) {
            case BE_WAITING:
                dataNode.setStatus(DataNodeStatus.UNKNOWN);
                break;
            case BE_DEAD:
                master.getDataNodeManager().getDataNodeMap().remove(dataNodeId);
                master.getChunkManager().batchAddPendingChunk(dataNode.getChunks());
                master.getChunkManager().clearChunk4SingleDataNode(dataNode);
                break;
            default:
        }
        return null;
    }
}
