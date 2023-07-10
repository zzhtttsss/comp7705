package comp7705.chunkserver;

import lombok.Data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
@Data
public class ChunkserverConfig {

    public static final ChunkserverConfig CHUNKSERVER_CONFIG;

    private List<String> masterGroupAddresses;

    private String masterGroupAddressesString;

    private String masterGroupId;

    private int chunkserverPort;



    static {
        CHUNKSERVER_CONFIG = new ChunkserverConfig();
    }

    public ChunkserverConfig() {
        InputStream stream = this.getClass().getResourceAsStream("/chunkserver.properties");
        if (stream == null) {
            throw new RuntimeException("chunkserver.properties not found");
        }
        Properties properties = new Properties();
        try {
            properties.load(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        masterGroupAddressesString = properties.getProperty("master.group.addresses");
        masterGroupAddresses = Arrays.asList(masterGroupAddressesString.split(","));
        masterGroupId = properties.getProperty("master.group.id");
        chunkserverPort = Integer.parseInt(properties.getProperty("chunkserver.port"));

    }
}