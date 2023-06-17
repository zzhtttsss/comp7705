package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.FileType;
import org.comp7705.metadata.FileNode;
import org.comp7705.protocol.definition.StatResponse;

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
    public Message apply() throws Exception {
        FileNode fileNode = master.getNamespaceManager().statFileNode(this.path);
        return StatResponse.newBuilder().setFileName(fileNode.getName()).setIsFile(fileNode.getType() == FileType.FILE)
                .setSize(fileNode.getSize()).build();
    }
}

