package org.comp7705.metadata;

import lombok.Data;

import java.util.Set;

@Data
public class Chunk {
    private String id;
    private Set<DataNode> dataNodes;
    private Set<DataNode> pendingDataNodes;

    public Chunk(String id) {
        this.id = id;
    }
}
