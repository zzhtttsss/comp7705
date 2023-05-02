package org.comp7705.metadata;

import lombok.Data;
import org.comp7705.common.FileStatus;
import org.comp7705.common.FileType;
import org.comp7705.util.StringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.comp7705.constant.Common.*;

@Data
public class FileNode {
    public static AtomicLong nextFileNodeId = new AtomicLong(1L);
    public static final long CHUNK_SIZE = 64 * MB;
    private String id;
    private String name;
    private FileNode parentNode;
    private Map<String, FileNode> childNodes;
    private ArrayList<String> chunks;
    private long size;
    private FileType type;
    private long delTime;
    private FileStatus status;

    public FileNode(String name, FileNode parentNode, long size, FileType type) {
        this.name = name;
        this.parentNode = parentNode;
        this.size = size;
        this.type = type;
        this.status = FileStatus.ALIVE;
        this.id = UUID.randomUUID().toString();
        this.delTime = 0;
        if (type == FileType.DIRECTORY) {
            this.childNodes = new HashMap<>();
        }
        else {
            initChunks(size);
        }

    }

    private void initChunks(long size) {
        int nums = (int) Math.ceil((double) size / (double) CHUNK_SIZE);
        this.chunks = new ArrayList<>(nums);
        for (int i = 0; i < nums; i++) {
            this.chunks.add(StringUtil.concatString(this.id, CHUNK_ID_DELIMITER, String.valueOf(i)));
        }
    }

}
