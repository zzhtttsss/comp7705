package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.SendStatus;
import org.comp7705.metadata.DataNode;
import org.comp7705.protocol.definition.SendType;

import java.util.Deque;
import java.util.List;

@Data
@Slf4j
public class AllocateChunksOperation implements Operation {

    private static final Master master = Master.MASTER;

    private String id;
    private int[] senderPlan;
    private int[] receiverPlan;
    private List<String> chunkIds;
    private List<String> dataNodeIds;
    private int batchLen;

    public AllocateChunksOperation(String id, int[] senderPlan, int[] receiverPlan, List<String> chunkIds,
                                   List<String> dataNodeIds, int batchLen) {
        this.id = id;
        this.senderPlan = senderPlan;
        this.receiverPlan = receiverPlan;
        this.chunkIds = chunkIds;
        this.dataNodeIds = dataNodeIds;
        this.batchLen = batchLen;
    }

    @Override
    public Message apply() throws Exception {
        batchApplyPlan2Chunk(receiverPlan, chunkIds, dataNodeIds);
        batchApplyPlan2DataNode(receiverPlan, senderPlan, chunkIds, dataNodeIds);
        Deque<String> queue = master.getChunkManager().getPendingChunkQueue();
        for (int i = 0; i < batchLen; i++) {
            queue.poll();
        }
        return null;
    }

    private void batchApplyPlan2Chunk(int[] plan, List<String> chunkIds, List<String> dataNodeIds) {
        for (int i = 0; i < plan.length; i++) {
            master.getChunkManager().getChunkMap().get(chunkIds.get(i)).getPendingDataNodes()
                    .add(dataNodeIds.get(plan[i]));
        }
    }

    private void batchApplyPlan2DataNode(int[] receiverPlan, int[] senderPlan, List<String> chunkIds,
                                         List<String> dataNodeIds) {
        for (int i = 0; i < senderPlan.length; i++) {
            DataNode.ChunkSendInfo info = new DataNode.ChunkSendInfo(chunkIds.get(i), dataNodeIds.get(receiverPlan[i]),
                    SendType.COPY);
            master.getDataNodeManager().getDataNodeMap().get(dataNodeIds.get(senderPlan[i])).getFutureSendChunks()
                    .put(info, SendStatus.TO_BE_INFORMED.getType());
        }
    }
}
