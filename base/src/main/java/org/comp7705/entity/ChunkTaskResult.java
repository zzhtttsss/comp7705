package org.comp7705.entity;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ChunkTaskResult {

    private String chunkId;
    private List<String> failDataNodes;
    private List<String> successDataNodes;
    private int sendType;
}
