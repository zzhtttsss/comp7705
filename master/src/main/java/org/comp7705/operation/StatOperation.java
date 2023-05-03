package org.comp7705.operation;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.FileType;

import static org.comp7705.Master.MASTER;


@Slf4j
@Data
public class StatOperation implements Operation{
    private Master master;

    private String id;
    private String path;

    public StatOperation(String id, String path) {
        this.id = id;
        this.path = path;
        this.master = MASTER;
    }

    @Override
    public Object apply() throws Exception {
        return master.getNamespaceManager().statFileNode(this.path);
    }
}

