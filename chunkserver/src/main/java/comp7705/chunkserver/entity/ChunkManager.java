package comp7705.chunkserver.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public class ChunkManager {

    private static Map<String, Chunk> chunkMap = new ConcurrentHashMap<>();

    public static void addPendingChunk(Chunk chunk) {
        chunkMap.put(chunk.getId(), chunk);
    }

    public static void completeChunk(Chunk chunk) {
        chunk.setComplete(true);
        chunkMap.put(chunk.getId(), chunk);
    }

    public static List<String> getAllChunkIds() {
        List<String> ids = new ArrayList<>();
        for (Map.Entry<String, Chunk> entry : chunkMap.entrySet()) {
            if (entry.getValue().isComplete()) {
                ids.add(entry.getKey());
            }
        }
        return ids;
    }

}
