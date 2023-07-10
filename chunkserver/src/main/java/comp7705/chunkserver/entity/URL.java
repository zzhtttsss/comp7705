package comp7705.chunkserver.entity;

import org.comp7705.constant.Const;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Reuze
 * @Date 26/05/2023
 */
@Setter
@Getter
@ToString
public class URL {

    String path;
    String ip;
    int port;

    public String getAddr() {
        if (ip == null || port == 0) {
            return "";
        }
        return ip + ":" + port;
    }

    public String getZkPath() {
        if (getAddr().equals("")) {
            return Const.ROOT_PATH + "/" + path;
        }
        return Const.ROOT_PATH + "/" + path + "/" + getAddr();
    }
}
