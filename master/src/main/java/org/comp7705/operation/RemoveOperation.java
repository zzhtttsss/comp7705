package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.FileType;
import org.comp7705.protocol.definition.RemoveResponse;

import static org.comp7705.Master.MASTER;


@Slf4j
@Data
public class RemoveOperation implements Operation{
    private Master master;

    private String id;
    private String path;

    public RemoveOperation(String id, String path) {
        this.id = id;
        this.path = path;
        this.master = MASTER;
    }

    @Override
    public Message apply() throws Exception {
        master.getNamespaceManager().removeFileNode(this.path);
        return RemoveResponse.newBuilder().build();
    }
}
