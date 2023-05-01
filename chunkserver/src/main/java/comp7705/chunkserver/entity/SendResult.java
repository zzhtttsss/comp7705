package comp7705.chunkserver.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
@Getter
@Setter
public class SendResult {

    private String chunkId;
    private String address;
    private int sendType;

    public SendResult(String chunkId, String address, int sendType) {
        this.chunkId = chunkId;
        this.address = address;
        this.sendType = sendType;
    }

}
