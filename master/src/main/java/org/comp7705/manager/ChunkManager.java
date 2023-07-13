package org.comp7705.manager;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.protocol.definition.SendType;
import org.comp7705.entity.ChunkTaskResult;
import org.comp7705.metadata.Chunk;
import org.comp7705.metadata.DataNode;
import org.comp7705.operation.HeartbeatOperation;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Slf4j
public class ChunkManager {
    private final Map<String, Chunk> chunkMap = new ConcurrentHashMap<String, Chunk>();

    private final Deque<String> pendingChunkQueue = new ArrayDeque<>();


    private ChunkManager() {
    }

    public static ChunkManager getInstance() {
        return ChunkManagerEnum.INSTANCE.getInstance();
    }

    public enum ChunkManagerEnum {
        INSTANCE;

        private final ChunkManager chunkManager;

        ChunkManagerEnum() {
            chunkManager = new ChunkManager();
        }

        public ChunkManager getInstance() {
            return chunkManager;
        }
    }

    public void addChunk(Chunk chunk) {
        chunkMap.put(chunk.getId(), chunk);
    }

    public void batchAddChunks(List<Chunk> chunks) {
        for (Chunk chunk : chunks) {
            chunkMap.put(chunk.getId(), chunk);
        }
    }

    public void batchClearPendingDataNodes(List<String> chunkIds) {
        for (String id : chunkIds) {
            if (chunkMap.containsKey(id)) {
                chunkMap.get(id).getPendingDataNodes().clear();
            }
        }
    }

    public void batchUpdatePendingDataNodes(List<ChunkTaskResult> chunkTaskResults) {
        for (ChunkTaskResult chunkTaskResult : chunkTaskResults) {
            if (chunkMap.containsKey(chunkTaskResult.getChunkId())) {
                for (String id : chunkTaskResult.getSuccessDataNodes()) {
                    if (chunkMap.get(chunkTaskResult.getChunkId()) == null) {
                        log.error("chunk is null, chunkId: {}", chunkTaskResult.getChunkId());
                    }
                    chunkMap.get(chunkTaskResult.getChunkId()).getDataNodes().add(id);
                    chunkMap.get(chunkTaskResult.getChunkId()).getPendingDataNodes().remove(id);

                }
                for (String id : chunkTaskResult.getFailDataNodes()) {
                    pendingChunkQueue.add(chunkTaskResult.getChunkId());
                    chunkMap.get(chunkTaskResult.getChunkId()).getPendingDataNodes().remove(id);
                }
            }

        }
    }

    public void updateChunk4Heartbeat(HeartbeatOperation heartbeatOperation) {
        for (DataNode.ChunkSendInfo info : heartbeatOperation.getSuccessInfos()) {
            if (chunkMap.containsKey(info.getChunkId())) {
                chunkMap.get(info.getChunkId()).getPendingDataNodes().remove(info.getDataNodeId());
                chunkMap.get(info.getChunkId()).getDataNodes().add(info.getDataNodeId());
                if (info.getSendType() == SendType.MOVE) {
                    chunkMap.get(info.getChunkId()).getDataNodes().remove(heartbeatOperation.getDataNodeId());
                }
            }
        }
        for (DataNode.ChunkSendInfo info : heartbeatOperation.getFailInfos()) {
            if (chunkMap.containsKey(info.getChunkId())) {
                chunkMap.get(info.getChunkId()).getPendingDataNodes().remove(info.getDataNodeId());
                if (info.getSendType() == SendType.MOVE || info.getSendType() == SendType.DELETE) {
                    continue;
                }
                pendingChunkQueue.add(info.getChunkId());
            }
        }
        for (String chunkId : heartbeatOperation.getInvalidChunks()) {
            if (chunkMap.containsKey(chunkId)) {
                pendingChunkQueue.add(chunkId);
            }
        }
    }


//    // getPendingChunks get a batch of Chunk's id from the pendingChunkQueue. The
//// batch size is the minimum of the current len of the pendingChunkQueue and
//// the maximum size.
//    func getPendingChunks() []string {
//        var (
//                maxCount  = viper.GetInt(common.ChunkDeadChunkCopyThreshold)
//                copyCount int
//	)
//        if pendingChunkQueue.Len() > maxCount {
//            copyCount = maxCount
//        } else {
//            copyCount = pendingChunkQueue.Len()
//        }
//        batchTs := pendingChunkQueue.BatchTop(copyCount)
//        batchChunkIds := make([]string, len(batchTs))
//        for i := 0; i < len(batchTs); i++ {
//            batchChunkIds[i] = batchTs[i].String()
//        }
//        return batchChunkIds
//    }

    public List<String> getPendingChunks() {
        List<String> batchChunkIds = new ArrayList<>();
        int maxCount = 32;
        int copyCount = Math.min(pendingChunkQueue.size(), maxCount);
        for (int i = 0; i < copyCount; i++) {
            batchChunkIds.add(pendingChunkQueue.poll());
        }
        return batchChunkIds;
    }

    public void batchAddPendingChunk(Collection<String> chunkIds) {
        this.pendingChunkQueue.addAll(chunkIds);
    }

    public void clearChunk4SingleDataNode(DataNode dataNode) {
        for (String chunkId: dataNode.getChunks()) {
            chunkMap.get(chunkId).getDataNodes().remove(dataNode.getId());
        }
    }


    public List<String> batchFilterChunk(List<String> chunkIds) {
        List<String> result = new ArrayList<>();
        for (String chunkId : chunkIds) {
            if (chunkMap.containsKey(chunkId)) {
                if (chunkMap.get(chunkId).getDataNodes().size() +
                        chunkMap.get(chunkId).getPendingDataNodes().size() < 3) {
                    result.add(chunkId);
                }
            }
        }
        return result;
    }

    public void batchApplyPlan2Chunk(List<Integer> plan, List<String> chunkIds, List<String> dataNodeIds) {
        for (int i = 0; i < plan.size(); i++) {
            chunkMap.get(chunkIds.get(i)).getPendingDataNodes().add(dataNodeIds.get(plan.get(i)));
        }
    }

//    // getStoreState gets the state of all DataNode which store target Chunk for all
//// given Chunk. We need to check both pendingDataNodes and dataNodes of a Chunk.
//    func getStoreState(chunkIds []string, dataNodeIds []string) [][]bool {
//        updateChunksLock.RLock()
//        defer updateChunksLock.RUnlock()
//        isStore := make([][]bool, len(chunkIds))
//        for i := range isStore {
//            isStore[i] = make([]bool, len(dataNodeIds))
//        }
//        dnIndexMap := make(map[string]int)
//        for i, id := range dataNodeIds {
//            dnIndexMap[id] = i
//        }
//        for i, id := range chunkIds {
//            chunk := chunksMap[id]
//            dataNodes := chunk.dataNodes.ToSlice()
//            pendingDataNodes := chunk.pendingDataNodes.ToSlice()
//            for _, dnId := range dataNodes {
//                isStore[i][dnIndexMap[dnId.(string)]] = true
//            }
//            for _, pdnId := range pendingDataNodes {
//                isStore[i][dnIndexMap[pdnId.(string)]] = true
//            }
//        }
//        return isStore
//    }

    public boolean[][] getStoreState(List<String> chunkIds, List<String> dataNodeIds) {
        boolean[][] isStore = new boolean[chunkIds.size()][dataNodeIds.size()];
        Map<String, Integer> dnIndexMap = new HashMap<>();
        for (int i = 0; i < dataNodeIds.size(); i++) {
            dnIndexMap.put(dataNodeIds.get(i), i);
        }
        for (int i = 0; i < chunkIds.size(); i++) {
            Chunk chunk = chunkMap.get(chunkIds.get(i));
            for (String dnId : chunk.getDataNodes()) {
                isStore[i][dnIndexMap.get(dnId)] = true;
            }
            for (String pdnId : chunk.getPendingDataNodes()) {
                isStore[i][dnIndexMap.get(pdnId)] = true;
            }
        }
        return isStore;
    }
}
