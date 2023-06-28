package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import org.comp7705.metadata.DataNode;

import java.util.List;

@Data
public class HeartbeatOperation implements Operation{

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


    @Override
    public Message apply() throws Exception {


        return null;
    }
}
