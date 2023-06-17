package org.comp7705;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import org.apache.commons.io.FileUtils;
import org.comp7705.grpc.MasterGrpcHelper;
import org.comp7705.protocol.definition.CheckArgs4AddRequest;
import org.comp7705.raft.*;

import java.io.File;
import java.io.IOException;

public class MasterServer {

    private RaftGroupService raftGroupService;
    private Node node;
    private MasterStateMachine fsm;

    public MasterServer(final String dataPath, final String groupId, final PeerId serverId,
                         final NodeOptions nodeOptions) throws IOException {
        // init raft data path, it contains log,meta,snapshot
        FileUtils.forceMkdir(new File(dataPath));

        // here use same RPC server for raft and business. It also can be seperated generally
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        // GrpcServer need init marshaller
        MasterGrpcHelper.initGRpc();
        MasterGrpcHelper.setRpcServer(rpcServer);
        // register business processor
        rpcServer.registerProcessor(new MasterRequestProcessor<>
                (CheckArgs4AddRequest.class, this));
        // init state machine
        this.fsm = new MasterStateMachine();
        // set fsm to nodeOptions
        nodeOptions.setFsm(this.fsm);
        // set storage path (log,meta,snapshot)
        // log, must
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // meta, must
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        // snapshot, optional, generally recommended
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // init raft group service framework
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer);
        // start raft node
        this.node = this.raftGroupService.start();
    }

    public MasterStateMachine getFsm() {
        return this.fsm;
    }

    public Node getNode() {
        return this.node;
    }

    public RaftGroupService getRaftGroupService() {
        return this.raftGroupService;
    }

    /**
     * Redirect request to new leader
     */
//    public <T> T redirect(T t) {
//        final ValueResponse.Builder builder = ValueResponse.newBuilder().setSuccess(false);
//        if (this.node != null) {
//            final PeerId leader = this.node.getLeaderId();
//            if (leader != null) {
//                builder.setRedirect(leader.toString());
//            }
//        }
//        return builder.build();
//    }

}
