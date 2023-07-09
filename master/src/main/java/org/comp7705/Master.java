package org.comp7705;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.manager.ChunkManager;
import org.comp7705.manager.DataNodeManager;
import org.comp7705.manager.NamespaceManager;
import org.comp7705.service.HeartbeatService;

@Slf4j
@Getter
public class Master {
    public static final Master MASTER;
    private NamespaceManager namespaceManager;
    private DataNodeManager dataNodeManager;
    private ChunkManager chunkManager;
    @Setter
    private MasterServer masterServer;
    @Setter
    private HeartbeatService heartbeatService;

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
