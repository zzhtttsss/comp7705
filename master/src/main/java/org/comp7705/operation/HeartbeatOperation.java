package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import org.comp7705.Master;
import org.comp7705.metadata.DataNode;
import org.comp7705.protocol.definition.ChunkInfo;
import org.comp7705.protocol.definition.HeartbeatResponse;

import java.util.ArrayList;
import java.util.List;

import static org.comp7705.Master.MASTER;

@Data
public class HeartbeatOperation implements Operation{
    private static final Master master = MASTER;

    private String id;
    private String dataNodeId;
    private List<String> chunkIds;
    private int ioLoad;
    private long fullCapacity;
    private long usedCapacity;
    private List<DataNode.ChunkSendInfo> successInfos;
    private List<DataNode.ChunkSendInfo> failInfos;
    private List<String> invalidChunks;
    private boolean isReady;

    public HeartbeatOperation(String id, String dataNodeId, List<String> chunkIds, int ioLoad, long fullCapacity,
                              long usedCapacity, List<DataNode.ChunkSendInfo> successInfos,
                              List<DataNode.ChunkSendInfo> failInfos, List<String> invalidChunks, boolean isReady) {
        this.id = id;
        this.dataNodeId = dataNodeId;
        this.chunkIds = chunkIds;
        this.ioLoad = ioLoad;
        this.fullCapacity = fullCapacity;
        this.usedCapacity = usedCapacity;
        this.successInfos = successInfos;
        this.failInfos = failInfos;
        this.invalidChunks = invalidChunks;
        this.isReady = isReady;
    }

    @Override
    public Message apply() throws Exception {
        List<DataNode.ChunkSendInfo> nextChunkInfos =
                master.getDataNodeManager().updateDataNode4Heartbeat(this);
        master.getChunkManager().updateChunk4Heartbeat(this);

        return HeartbeatResponse.newBuilder()
                .addAllChunkInfos(convChunkInfo(nextChunkInfos))
                .addAllDataNodeAddress(getDataNodeAddresses(nextChunkInfos))
                .build();
    }

    private List<ChunkInfo> convChunkInfo(List<DataNode.ChunkSendInfo> chunkSendInfos) {
        List<ChunkInfo> chunkInfos = new ArrayList<>();
        for (DataNode.ChunkSendInfo chunkSendInfo : chunkSendInfos) {
            ChunkInfo chunkInfo = ChunkInfo.newBuilder()
                    .setChunkId(chunkSendInfo.getChunkId())
                    .setDataNodeId(chunkSendInfo.getDataNodeId())
                    .setSendType(chunkSendInfo.getSendType())
                    .build();
            chunkInfos.add(chunkInfo);
        }
        return chunkInfos;
    }

    private List<String> getDataNodeAddresses(List<DataNode.ChunkSendInfo> chunkSendInfos) {
        List<String> adds = new ArrayList<>();
        for (DataNode.ChunkSendInfo info : chunkSendInfos) {
            DataNode node = master.getDataNodeManager().getDataNodeMap().get(info.getDataNodeId());
            if (node != null) {
                adds.add(node.getAddress());
            }
        }
        return adds;
    }
}
