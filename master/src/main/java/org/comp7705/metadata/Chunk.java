package org.comp7705.metadata;

import lombok.Data;

import java.util.HashSet;
import java.util.Set;

@Data
public class Chunk {
    private String id;
    private Set<String> dataNodes;
    private Set<String> pendingDataNodes;

    public Chunk(String id, Set<String> pendingDataNodes) {
        this.id = id;
        this.pendingDataNodes = pendingDataNodes;
        dataNodes = new HashSet<>();
    }
}
