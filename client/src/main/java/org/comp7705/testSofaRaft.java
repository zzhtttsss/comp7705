package org.comp7705;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import org.comp7705.grpc.MasterGrpcHelper;
import org.comp7705.protocol.definition.MkdirRequest;

import java.util.concurrent.CountDownLatch;

public class testSofaRaft {

    public static void main(final String[] args) throws Exception {
//        if (args.length != 2) {
//            System.out.println("Usage : java com.alipay.sofa.jraft.example.counter.CounterClient {groupId} {conf}");
//            System.out
//                    .println("Example: java com.alipay.sofa.jraft.example.counter.CounterClient counter 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
//            System.exit(1);
//        }
        final String groupId = "master";
//        final String confStr = "172.18.0.12:8081";
        final String confStr = "127.0.0.1:8081";

        MasterGrpcHelper.initGRpc();

        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());

        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);
        final int n = 1;
        final CountDownLatch latch = new CountDownLatch(n);
        final long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            checkArg4Add(cliClientService, leader, latch);
        }
        latch.await();
        System.out.println(n + " ops, cost : " + (System.currentTimeMillis() - start) + " ms.");
        System.exit(0);
    }

    private static void checkArg4Add(final CliClientServiceImpl cliClientService, final PeerId leader,
                                     CountDownLatch latch) throws RemotingException,
            InterruptedException {
//        CheckArgs4AddRequest request = CheckArgs4AddRequest.newBuilder().setPath("/").setFileName("aa").setSize(1024 * 1024 * 128).build();
////        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, (result, err) -> {
////            if (err == null) {
////                latch.countDown();
////                CheckArgs4AddResponse response = (CheckArgs4AddResponse) result;
////                System.out.println("incrementAndGet result:" + response.getFileNodeId());
////            } else {
////                err.printStackTrace();
////                latch.countDown();
////            }
////        }, 10000);
//        cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), request, 10000);
//        latch.countDown();

        MkdirRequest request = MkdirRequest.newBuilder()
                .setPath("/")
                .setDirName("a")
                .build();
        cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), request, 10000);
        latch.countDown();

    }
}
