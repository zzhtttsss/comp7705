package org.comp7705.operation;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.FileType;

import static org.comp7705.Master.MASTER;


@Slf4j
@Data
public class MkdirOperation implements Operation{
    private Master master;

    private String id;
    private String path;
    private String filename;

    public MkdirOperation(String id, String path, String filename) {
        this.id = id;
        this.path = path;
        this.filename = filename;
        this.master = MASTER;
    }

    @Override
    public Object apply() throws Exception {
        return master.getNamespaceManager().addFileNode(this.path, this.filename, FileType.DIRECTORY, 0);
    }
}
