package org.comp7705.operation;

import com.google.protobuf.Message;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.common.FileType;
import org.comp7705.metadata.FileNode;
import org.comp7705.protocol.definition.FileInfo;
import org.comp7705.protocol.definition.ListResponse;

import java.util.List;

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
    public Message apply() throws Exception {
        List<FileNode> fileNodes = master.getNamespaceManager().listFileNode(this.path);
        ListResponse.Builder builder = ListResponse.newBuilder();
        for (FileNode fileNode : fileNodes) {
            builder.addFiles(FileInfo.newBuilder().setFileName(fileNode.getName())
                    .setIsFile(fileNode.getType() == FileType.FILE).build());
        }
        return ListResponse.newBuilder().build();
    }
}
