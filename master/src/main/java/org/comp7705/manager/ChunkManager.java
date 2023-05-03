package org.comp7705.manager;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.entity.ChunkTaskResult;
import org.comp7705.metadata.Chunk;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
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
                    chunkMap.get(chunkTaskResult.getChunkId()).getDataNodes().add(id);
                }
                for (String ignored : chunkTaskResult.getFailDataNodes()) {
                    pendingChunkQueue.add(chunkTaskResult.getChunkId());
                }
            }
        }
    }




}