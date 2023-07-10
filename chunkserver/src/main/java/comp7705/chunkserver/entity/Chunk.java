package comp7705.chunkserver.entity;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * @author Reuze
 * @Date 01/05/2023
 */

@Data
public class Chunk {

    private String id;

    private String fileId;

    private String index;

    private boolean isComplete;

    private long addTime;

    // chunkId = fileId_index
    public Chunk(String id) {
        this.id = id;
        String[] strings = id.split("_");
        this.fileId = strings[0];
        this.index = strings[1];
        this.isComplete = false;
        this.addTime = System.currentTimeMillis();
    }

    public Chunk(String id, long modTime) {
        this.id = id;
        String[] strings = id.split("_");
        this.fileId = strings[0];
        this.index = strings[1];
        this.isComplete = false;
        this.addTime = modTime;
    }

    public Chunk(String id, String fileId, String index) {
        this.id = id;
        this.fileId = fileId;
        this.index = index;
        this.isComplete = false;
        this.addTime = System.currentTimeMillis();
    }

}
