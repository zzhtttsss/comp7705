package org.comp7705.client.services;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.grpc.MasterGrpcHelper;
import org.comp7705.protocol.definition.*;

import java.util.List;
import java.util.concurrent.TimeoutException;

@Slf4j
@Data
public class MasterClient {
    private String groupId = "master";
    private String host = "localhost";
    private int port = 8081;
    private CliClientServiceImpl cliClientService;

    private static final int TIME_OUT = 50000;

    public static final MasterClient masterClient = new MasterClient();

    private MasterClient() {
        final String groupId = "master";
//        final String confStr = "172.18.0.12:8081";
        final String confStr = "127.0.0.1:8081";
        MasterGrpcHelper.initGRpc();

        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());

        try {
            if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
                throw new IllegalStateException("Refresh leader failed");
            }
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }


    }

    public void mkdir(String path, String dirName) throws RemotingException, InterruptedException, TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        MkdirRequest request = MkdirRequest.newBuilder()
                .setPath(path)
                .setDirName(dirName)
                .build();
        cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public void move(String src, String des) throws RemotingException, InterruptedException, TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        MoveRequest request = MoveRequest.newBuilder()
                .setSourcePath(src)
                .setTargetPath(des)
                .build();
        cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public void remove(String src) throws RemotingException, InterruptedException, TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        RemoveRequest request = RemoveRequest.newBuilder()
                .setPath(src)
                .build();
        cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public void rename(String src, String des) throws RemotingException, InterruptedException, TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        RenameRequest request = RenameRequest.newBuilder()
                .setPath(src)
                .setNewName(des)
                .build();
        cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public void stat(String src, boolean isLatest) throws RemotingException, InterruptedException, TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        MkdirRequest request = MkdirRequest.newBuilder()
                .setPath(src)
                .build();
        cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public void list(String path, boolean isLatest) throws RemotingException, InterruptedException, TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        MkdirRequest request = MkdirRequest.newBuilder()
                .setPath(path)
                .build();
        cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public CheckArgs4AddResponse checkArgs4Add(String filename, String path, long filesize)
            throws RemotingException, InterruptedException, TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        CheckArgs4AddRequest request = CheckArgs4AddRequest.newBuilder()
                .setPath(path)
                .setFileName(filename)
                .setSize(filesize)
                .build();
        return (CheckArgs4AddResponse) cliClientService.getRpcClient()
                .invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public GetDataNodes4AddResponse getDataNodes4Add(String fileNodeId, int chunkNum)
            throws RemotingException, InterruptedException, TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        GetDataNodes4AddRequest request = GetDataNodes4AddRequest.newBuilder()
                .setFileNodeId(fileNodeId)
                .setChunkNum(chunkNum)
                .build();
        return (GetDataNodes4AddResponse) cliClientService.getRpcClient()
                .invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public Callback4AddResponse callBack4Add(String fileNodeId, String filepath, List<ChunkInfo4Add> infos,
                                             List<String> failChunkIds) throws RemotingException,
            InterruptedException, TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        Callback4AddRequest request = Callback4AddRequest.newBuilder()
                .setFileNodeId(fileNodeId)
                .setFilePath(filepath)
                .addAllInfos(infos)
                .addAllFailChunkIds(failChunkIds)
                .build();
        return (Callback4AddResponse) cliClientService.getRpcClient()
                .invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public CheckArgs4GetResponse checkArgs4Get(String path) throws RemotingException, InterruptedException,
            TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        CheckArgs4GetRequest request = CheckArgs4GetRequest.newBuilder()
                .setPath(path)
                .build();
        return (CheckArgs4GetResponse) cliClientService.getRpcClient()
                .invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public GetDataNodes4GetResponse getDataNodes4Get(String fileNodeId, int chunkIndex) throws RemotingException,
            InterruptedException, TimeoutException {
        final PeerId leader = refreshAndGetLeader();
        GetDataNodes4GetRequest request = GetDataNodes4GetRequest.newBuilder()
                .setFileNodeId(fileNodeId)
                .setChunkIndex(chunkIndex)
                .build();
        return (GetDataNodes4GetResponse) cliClientService.getRpcClient()
                .invokeSync(leader.getEndpoint(), request, TIME_OUT);
    }

    public PeerId refreshAndGetLeader() throws InterruptedException, TimeoutException {
        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 5000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }
        return RouteTable.getInstance().selectLeader(groupId);

    }

}
