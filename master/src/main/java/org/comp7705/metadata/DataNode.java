package org.comp7705.metadata;

import lombok.Data;
import org.comp7705.common.DataNodeStatus;
import org.comp7705.protocol.definition.SendType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.comp7705.constant.Common.MB;

@Data
public class DataNode {

    private String id;
    private DataNodeStatus status;
    private String address;
    private Set<String> chunks;
    private int ioLoad;
    private long fullCapacity;
    private long usedCapacity;
    private Map<ChunkSendInfo, Integer> futureSendChunks;
    private long lastHeartbeatTime;
    private long tempChunkSize;

    public DataNode(String id, DataNodeStatus status, String address, long fullCapacity) {
        this.id = id;
        this.status = status;
        this.address = address;
        this.chunks = new HashSet<>();
        this.ioLoad = 0;
        this.fullCapacity = fullCapacity;
        this.usedCapacity = 0;
        this.futureSendChunks = new HashMap<>();
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.tempChunkSize = 0;
    }

    public void addTempChunkSize() {
        this.tempChunkSize += 64 * MB;
    }

    @Data
    public static class ChunkSendInfo {
        private String chunkId;
        private String dataNodeId;
        private SendType sendType;

        public ChunkSendInfo(String chunkId, String dataNodeId, SendType sendType) {
            this.chunkId = chunkId;
            this.dataNodeId = dataNodeId;
            this.sendType = sendType;
        }
    }

    public int calUsage() {
        return calUsage(this.usedCapacity, this.fullCapacity, this.tempChunkSize);
    }

    public static int calUsage(long usedCapacity, long fullCapacity, long temp) {
        return (int) ((usedCapacity + temp) * 100 / fullCapacity);
    }
}
