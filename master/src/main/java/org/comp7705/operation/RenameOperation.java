package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.FileType;
import org.comp7705.protocol.definition.RemoveResponse;
import org.comp7705.protocol.definition.RenameResponse;

import static org.comp7705.Master.MASTER;


@Slf4j
@Data
public class RenameOperation implements Operation{
    private Master master;

    private String id;
    private String path;
    private String newName;

    public RenameOperation(String id, String path, String newName) {
        this.id = id;
        this.path = path;
        this.newName = newName;
        this.master = MASTER;
    }

    @Override
    public Message apply() throws Exception {
        master.getNamespaceManager().renameFileNode(this.path, this.newName);
        return RenameResponse.newBuilder().build();
    }
}
