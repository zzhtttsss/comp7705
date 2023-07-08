package comp7705.chunkserver.entity;

import lombok.Getter;
import lombok.Setter;
import org.comp7705.protocol.definition.SendType;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
@Getter
@Setter
public class SendResult {

    private String chunkId;
    private String address;
    private SendType sendType;

    public SendResult(String chunkId, String address, SendType sendType) {
        this.chunkId = chunkId;
        this.address = address;
        this.sendType = sendType;
    }

}
