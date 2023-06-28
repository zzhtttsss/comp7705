package org.comp7705.manager;

import org.comp7705.common.DataNodeStatus;
import org.comp7705.common.SendStatus;
import org.comp7705.common.SendType;
import org.comp7705.entity.ChunkTaskResult;
import org.comp7705.metadata.DataNode;
import org.comp7705.operation.HeartbeatOperation;

import java.util.*;

public class DataNodeManager {

    public Map<String, DataNode> dataNodeMap = new HashMap<>();

    public PriorityQueue<DataNode> dataNodeHeap = new PriorityQueue<>((o1, o2) -> o2.calUsage() - o1.calUsage());


    private DataNodeManager() {
    }

    public static DataNodeManager getInstance() {
        return DataNodeManagerEnum.INSTANCE.getInstance();
    }

    public enum DataNodeManagerEnum {
        INSTANCE;

        private final DataNodeManager dataNodeManager;

        DataNodeManagerEnum() {
            dataNodeManager = new DataNodeManager();
        }

        public DataNodeManager getInstance() {
            return dataNodeManager;
        }
    }

    public ArrayList<ArrayList<DataNode>> batchAllocateDataNodes(int chunkNum) {
        ArrayList<ArrayList<DataNode>> allocateResult = new ArrayList<>();
        ArrayList<DataNode> aliveDataNodes = new ArrayList<>();
        dataNodeMap.forEach((id, dataNode) -> {
            if (dataNode.getStatus() == DataNodeStatus.ALIVE) {
                aliveDataNodes.add(dataNode);
            }
        });

        for (int i = 0; i < chunkNum; i++) {
            dataNodeHeap.clear();
            for (DataNode dataNode : aliveDataNodes) {
                adjust4batch(dataNode);
            }
            allocateResult.add(new ArrayList<>(dataNodeHeap));
        }

        for (DataNode dataNode : aliveDataNodes) {
            dataNode.setTempChunkSize(0);
        }

        return allocateResult;
    }

    private void adjust4batch(DataNode dataNode) {
        if (dataNodeHeap.size() < 3) {
            dataNodeHeap.add(dataNode);
            dataNode.addTempChunkSize();
        }
        else {
            DataNode minUsageDataNode = dataNodeHeap.peek();
            if (minUsageDataNode.calUsage() > dataNode.calUsage()) {
                dataNodeHeap.poll();
                dataNodeHeap.add(dataNode);
                dataNode.addTempChunkSize();
            }
        }
    }

    public void batchAddChunks(List<ChunkTaskResult> chunkTaskResults) {
        for (ChunkTaskResult chunkTaskResult : chunkTaskResults) {
            for (String id : chunkTaskResult.getSuccessDataNodes()) {
                if (dataNodeMap.containsKey(id)) {
                    dataNodeMap.get(id).getChunks().add(chunkTaskResult.getChunkId());
                }
            }
        }
    }

    public ArrayList<DataNode> getSortedDataNodes(Set<String> set) {
        ArrayList<DataNode> dns = new ArrayList<>();
        for (String id : set) {
            if (dataNodeMap.containsKey(id)) {
                if (dataNodeMap.get(id).getStatus() == DataNodeStatus.ALIVE) {
                    dns.add(dataNodeMap.get(id));
                }
            }
        }
        dns.sort(Comparator.comparingInt(DataNode::getIoLoad));
        return dns;
    }

    public List<DataNode.ChunkSendInfo> updateDataNode4Heartbeat(HeartbeatOperation operation) {
        DataNode dataNode = dataNodeMap.get(operation.getDataNodeId());
        if (dataNode == null) {
            return null;
        }
        dataNode.setFullCapacity(operation.getFullCapacity());
        dataNode.setUsedCapacity(operation.getUsedCapacity());
        dataNode.setLastHeartbeatTime(System.currentTimeMillis());
        if (operation.isReady()) {
            dataNode.setStatus(DataNodeStatus.ALIVE);
        }
        dataNode.setIoLoad(operation.getIoLoad());
        for (DataNode.ChunkSendInfo info : operation.getSuccessInfos()) {
            dataNode.getFutureSendChunks().remove(info);
            if (info.getSendType() == SendType.MOVE || info.getSendType() == SendType.DELETE) {
                dataNode.getChunks().remove(info.getChunkId());
            }
            if (dataNodeMap.containsKey(info.getDataNodeId())) {
                dataNodeMap.get(info.getDataNodeId()).getChunks().add(info.getChunkId());
            }
        }
        for (DataNode.ChunkSendInfo info : operation.getFailInfos()) {
            dataNode.getFutureSendChunks().remove(info);
            if (info.getSendType() == SendType.MOVE || info.getSendType() == SendType.DELETE) {
                continue;
            }
            ChunkManager.getInstance().getPendingChunkQueue().push(info.getChunkId());
        }
        for (String chunkId : operation.getInvalidChunks()) {
            dataNode.getChunks().remove(chunkId);
        }

        ArrayList<DataNode.ChunkSendInfo> nextChunkInfos = new ArrayList<>();
        for (Map.Entry<DataNode.ChunkSendInfo, Integer> entry : dataNode.getFutureSendChunks().entrySet()) {
            if (entry.getValue() == SendStatus.TO_BE_INFORMED.getType()) {
                nextChunkInfos.add(entry.getKey());
                dataNode.getFutureSendChunks().put(entry.getKey(), SendStatus.TO_BE_SENT.getType());
            }
        }
        return nextChunkInfos;
    }
}
