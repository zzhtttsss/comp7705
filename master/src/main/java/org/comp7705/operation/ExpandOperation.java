package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.SendStatus;
import org.comp7705.metadata.DataNode;
import org.comp7705.protocol.definition.SendType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Slf4j
public class ExpandOperation implements Operation {

    private static final Master master = Master.MASTER;

    private String id;
    private Map<String, List<String>> senderPlan;
    private String receiverPlan;
    private List<String> chunkIds;

    public ExpandOperation(String id, Map<String, List<String>> senderPlan, String receiverPlan,
                           List<String> chunkIds) {
        this.id = id;
        this.senderPlan = senderPlan;
        this.receiverPlan = receiverPlan;
        this.chunkIds = chunkIds;
    }


//    func (e ExpandOperation) Apply() (interface{}, error) {
//        Logger.Infof("Apply expand operation with dataNode %s", e.ReceiverPlan)
//        updateMapLock.Lock()
//        for fromNodeId, targetChunks := range e.SenderPlan {
//            fromNode := dataNodeMap[fromNodeId]
//            for _, chunkId := range targetChunks {
//                newFutureSendPlan := ChunkSendInfo{
//                    ChunkId:    chunkId,
//                            DataNodeId: e.ReceiverPlan,
//                            SendType:   common.MoveSendType,
//                }
//                fromNode.FutureSendChunks[newFutureSendPlan] = common.WaitToInform
//            }
//        }
//        updateMapLock.Unlock()
//        updateChunksLock.Lock()
//        for _, chunkId := range e.ChunkIds {
//            if chunk, ok := chunksMap[chunkId]; ok {
//                chunk.pendingDataNodes.Add(e.ReceiverPlan)
//            }
//        }
//        updateChunksLock.Unlock()
//        return nil, nil
//    }

    @Override
    public Message apply() throws Exception {
        log.info("Apply expand operation with dataNode {}", receiverPlan);
        for (Map.Entry<String, List<String>> entry : senderPlan.entrySet()) {
            String fromNodeId = entry.getKey();
            List<String> targetChunks = entry.getValue();
            Map<DataNode.ChunkSendInfo, Integer> futureSendChunks = master.getDataNodeManager().getDataNodeMap()
                    .get(fromNodeId).getFutureSendChunks();
            for (String chunkId : targetChunks) {
                DataNode.ChunkSendInfo newFutureSendPlan = new DataNode.ChunkSendInfo(chunkId, receiverPlan,
                        SendType.MOVE);
                futureSendChunks.put(newFutureSendPlan, SendStatus.TO_BE_INFORMED.getType());
            }
            for (String chunkId : chunkIds) {
                if (master.getChunkManager().getChunkMap().containsKey(chunkId)) {
                    master.getChunkManager().getChunkMap().get(chunkId).getPendingDataNodes().add(receiverPlan);
                }
            }
        }
        return null;
    }
}
