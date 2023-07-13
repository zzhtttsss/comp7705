package org.comp7705;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.grpc.MasterGrpcHelper;
import org.comp7705.service.HeartbeatService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.comp7705.Master.MASTER;
import static org.comp7705.MasterConfig.MASTER_CONFIG;


@Slf4j
public class MasterStartup {

    private static final Logger logger = LoggerFactory.getLogger(MasterStartup.class);

    private static MasterServer masterServer;

    private static final MasterConfig config = MasterConfig.MASTER_CONFIG;

    public static void start() {
        // read config first
        MASTER.init();
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        start();
//        if (args.length != 4) {
//            System.out
//                    .println("Usage : java com.alipay.sofa.jraft.example.counter.CounterServer {dataPath} {groupId} {serverId} {initConf}");
//            System.out
//                    .println("Example: java com.alipay.sofa.jraft.example.counter.CounterServer /tmp/server1 counter 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
//            System.exit(1);
//        }
////        final String dataPath = args[0];
////        final String groupId = args[1];
////        final String serverIdStr = args[2];
////        final String initConfStr = args[3];

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
        if (!serverId.parse(config.getMasterServerId())) {
            throw new IllegalArgumentException("Fail to parse serverId:" + config.getMasterServerId());
        }
        final Configuration initConf = new Configuration();
        if (!initConf.parse(config.getMasterGroupAddressesString())) {
            throw new IllegalArgumentException("Fail to parse initConf:" + config.getMasterGroupAddressesString());
        }
        // set cluster configuration
        nodeOptions.setInitialConf(initConf);

        // start raft server
        masterServer = new MasterServer(config.getMasterDataPath(), config.getMasterGroupId(), serverId, nodeOptions);
        HeartbeatService heartbeatService = new HeartbeatService(masterServer);
        MASTER.setMasterServer(masterServer);
        MASTER.setHeartbeatService(heartbeatService);
        logger.info("Started counter server at port:"
                + masterServer.getNode().getNodeId().getPeerId().getPort());
        // GrpcServer need block to prevent process exit
        MasterGrpcHelper.blockUntilShutdown();
    }
}
