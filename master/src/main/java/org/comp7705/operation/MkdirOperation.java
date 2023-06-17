package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.FileType;
import org.comp7705.metadata.FileNode;
import org.comp7705.protocol.definition.MkDirResponse;

import static org.comp7705.Master.MASTER;


@Slf4j
@Data
public class MkdirOperation implements Operation{
    private static final Master master = MASTER;

    private String id;
    private String path;
    private String filename;

    public MkdirOperation(String id, String path, String filename) {
        this.id = id;
        this.path = path;
        this.filename = filename;
    }

    @Override
    public Message apply() throws Exception {
       master.getNamespaceManager().addFileNode(this.path, this.filename, FileType.DIRECTORY, 0);
       return MkDirResponse.newBuilder().build();
    }
}
