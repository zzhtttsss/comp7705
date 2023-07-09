package org.comp7705;

import lombok.Data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Data
public class MasterConfig {

    public static final MasterConfig MASTER_CONFIG;

    private int masterGrpcPort;

    private int expandThreshold;

    private List<String> masterGroupAddresses;

    private String masterGroupAddressesString;

    private String masterGroupId;

    private String masterServerId;

    private String masterDataPath;

    static {
        MASTER_CONFIG = new MasterConfig();
    }

    public MasterConfig() {
        InputStream stream = this.getClass().getResourceAsStream("/master.properties");
        if (stream == null) {
            throw new RuntimeException("master.properties not found");
        }
        Properties properties = new Properties();
        try {
            properties.load(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        masterGrpcPort = Integer.parseInt(properties.getProperty("master.grpc.port"));
        expandThreshold = Integer.parseInt(properties.getProperty("master.expand.threshold"));
        masterGroupAddressesString = properties.getProperty("master.group.addresses");
        masterGroupAddresses = Arrays.asList(masterGroupAddressesString.split(","));
        masterGroupId = properties.getProperty("master.group.id");
        masterServerId = properties.getProperty("master.server.id");
        masterDataPath = properties.getProperty("master.data.path");
    }
}
