package comp7705.chunkserver.entity;

import lombok.Data;

@Data
public class DataNode {
    private String id;
    private int futureChunkNum;
    private boolean isReady;
    private long ioLoad;

    public DataNode(String id, int futureChunkNum, boolean isReady, long ioLoad) {
        this.id = id;
        this.futureChunkNum = futureChunkNum;
        this.isReady = isReady;
        this.ioLoad = ioLoad;
    }
}
