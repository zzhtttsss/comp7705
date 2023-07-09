package org.comp7705.client;

import lombok.Data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Data
public class ClientConfig {

    public static final ClientConfig CLIENT_CONFIG;

    private List<String> masterGroupAddresses;

    private String masterGroupAddressesString;

    private String masterGroupId;

    static {
        CLIENT_CONFIG = new ClientConfig();
    }

    public ClientConfig() {
        InputStream stream = this.getClass().getResourceAsStream("/client.properties");
        if (stream == null) {
            throw new RuntimeException("client.properties not found");
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

    }
}