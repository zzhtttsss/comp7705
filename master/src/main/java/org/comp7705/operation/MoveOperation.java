package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.FileType;
import org.comp7705.protocol.definition.MoveResponse;

import static org.comp7705.Master.MASTER;


@Slf4j
@Data
public class MoveOperation implements Operation{
    private Master master;

    private String id;
    private String sourcePath;
    private String targetPath;


    public MoveOperation(String id, String sourcePath, String targetPath) {
        this.id = id;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.master = MASTER;
    }

    @Override
    public Message apply() throws Exception {
        master.getNamespaceManager().moveFileNode(this.sourcePath, this.targetPath);
        return MoveResponse.newBuilder().build();
    }
}
