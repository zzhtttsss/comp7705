package org.comp7705.operation;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.FileType;

import static org.comp7705.Master.MASTER;


@Slf4j
@Data
public class ListOperation implements Operation{
    private Master master;

    private String id;
    private String path;

    public ListOperation(String id, String path) {
        this.id = id;
        this.path = path;
        this.master = MASTER;
    }

    @Override
    public Object apply() throws Exception {
        return master.getNamespaceManager().listFileNode(this.path);
    }
}
