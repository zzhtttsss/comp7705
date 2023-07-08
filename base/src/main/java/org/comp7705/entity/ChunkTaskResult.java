package org.comp7705.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class ChunkTaskResult implements Serializable {

    private String chunkId;
    private List<String> failDataNodes;
    private List<String> successDataNodes;
    private int sendType;

    public ChunkTaskResult(String chunkId, List<String> failDataNodes, List<String> successDataNodes, int sendType) {
        this.chunkId = chunkId;
        this.failDataNodes = failDataNodes;
        this.successDataNodes = successDataNodes;
        this.sendType = sendType;
    }
}
