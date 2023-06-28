package comp7705.chunkserver.entity;

import lombok.Getter;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
@Getter
public class PendingChunk {

    private String chunkId;
    private String address;
    private int sendType;

    public PendingChunk(String chunkId, int sendType, String address) {
        this.chunkId = chunkId;
        this.sendType = sendType;
        this.address = address;
    }
}
