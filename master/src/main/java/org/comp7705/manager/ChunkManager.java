package org.comp7705.manager;

import org.comp7705.metadata.Chunk;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ChunkManager {
    private Map<String, Chunk> chunkMap = new ConcurrentHashMap<String, Chunk>();



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

}
