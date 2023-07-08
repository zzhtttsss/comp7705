package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.DataNodeStatus;
import org.comp7705.metadata.DataNode;
import org.comp7705.protocol.definition.DNRegisterResponse;

import java.util.HashSet;
import java.util.List;

@Data
@Slf4j
public class RegisterOperation implements Operation {

    private String id;
    private String dataNodeId;
    private String address;
    private List<String> chunkIds;
    private long fullCapacity;
    private long usedCapacity;
    private boolean isNeedExpand;

    public RegisterOperation(String id, String dataNodeId, String address, List<String> chunkIds,
                             long fullCapacity, long usedCapacity, boolean isNeedExpand) {
        this.id = id;
        this.dataNodeId = dataNodeId;
        this.address = address;
        this.chunkIds = chunkIds;
        this.fullCapacity = fullCapacity;
        this.usedCapacity = usedCapacity;
        this.isNeedExpand = isNeedExpand;
    }

    @Override
    public Message apply() throws Exception {
        DataNode dataNode = new DataNode(dataNodeId, isNeedExpand ? DataNodeStatus.COLD : DataNodeStatus.ALIVE,
                address, fullCapacity, usedCapacity, new HashSet<>(chunkIds));
        Master.MASTER.getDataNodeManager().getDataNodeMap().put(dataNodeId, dataNode);
        return DNRegisterResponse.newBuilder().setId(dataNodeId).setPendingCount(0).build();
    }
}
