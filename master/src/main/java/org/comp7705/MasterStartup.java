package org.comp7705;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import io.grpc.Server;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.grpc.MasterGrpcHelper;
import org.comp7705.raft.MasterStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.comp7705.Master.MASTER;
import static org.comp7705.MasterConfig.MASTER_CONFIG;


@Slf4j
public class MasterStartup {

    private static final Logger logger = LoggerFactory.getLogger(MasterStartup.class);

    private static MasterServer masterServer;

    public static void start() {
        // read config first
        MASTER.init();
        // The port on which the server should run
        int port = MASTER_CONFIG.getMasterGrpcPort();
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        start();
        if (args.length != 4) {
            System.out
                    .println("Usage : java com.alipay.sofa.jraft.example.counter.CounterServer {dataPath} {groupId} {serverId} {initConf}");
            System.out
                    .println("Example: java com.alipay.sofa.jraft.example.counter.CounterServer /tmp/server1 counter 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }
        final String dataPath = args[0];
        final String groupId = args[1];
        final String serverIdStr = args[2];
        final String initConfStr = args[3];

        final NodeOptions nodeOptions = new NodeOptions();
        // for test, modify some params
        // set election timeout to 1s
        nodeOptions.setElectionTimeoutMs(1000);
        // disable CLI service。
        nodeOptions.setDisableCli(false);
        // do snapshot every 30s
        nodeOptions.setSnapshotIntervalSecs(30);
        // parse server address
        final PeerId serverId = new PeerId();
        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }
        final Configuration initConf = new Configuration();
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        // set cluster configuration
        nodeOptions.setInitialConf(initConf);

        // start raft server
        masterServer = new MasterServer(dataPath, groupId, serverId, nodeOptions);
        logger.info("Started counter server at port:"
                + masterServer.getNode().getNodeId().getPeerId().getPort());
        // GrpcServer need block to prevent process exit
        MasterGrpcHelper.blockUntilShutdown();
    }
}
