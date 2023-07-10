package comp7705.chunkserver.entity;

import lombok.Getter;
import org.comp7705.protocol.definition.SendType;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
@Getter
public class PendingChunk {

    private String chunkId;
    private String address;
    private SendType sendType;

    public PendingChunk(String chunkId, SendType sendType, String address) {
        this.chunkId = chunkId;
        this.sendType = sendType;
        this.address = address;
    }
}
