package org.comp7705;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.manager.ChunkManager;
import org.comp7705.manager.DataNodeManager;
import org.comp7705.manager.NamespaceManager;

@Slf4j
@Getter
public class Master {
    public static final Master MASTER;
    private NamespaceManager namespaceManager;
    private DataNodeManager dataNodeManager;
    private ChunkManager chunkManager;

    static {
        MASTER = new Master();
    }

    private Master() {
    }

    public void init() {
        this.namespaceManager = NamespaceManager.getInstance();
        this.dataNodeManager = DataNodeManager.getInstance();
        this.chunkManager = ChunkManager.getInstance();
    }


}
