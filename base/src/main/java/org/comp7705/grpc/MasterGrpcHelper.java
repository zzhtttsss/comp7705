package org.comp7705.grpc;

import com.alipay.sofa.jraft.rpc.RpcServer;

import com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory;
import com.alipay.sofa.jraft.rpc.impl.MarshallerRegistry;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;
import org.comp7705.protocol.definition.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

public class MasterGrpcHelper {

    private static final Logger logger = LoggerFactory.getLogger(MasterGrpcHelper.class);

    public static RpcServer rpcServer;

    public static void initGRpc() {
//        GrpcRaftRpcFactory raftRpcFactory = (GrpcRaftRpcFactory) RpcFactoryHelper.rpcFactory();
//        raftRpcFactory.registerProtobufSerializer(CheckArgs4AddRequest.class.getName(),
//                CheckArgs4AddRequest.getDefaultInstance());
//        raftRpcFactory.registerProtobufSerializer(
//                CheckArgs4AddResponse.class.getName(),
//                CheckArgs4AddResponse.getDefaultInstance());
//        MarshallerRegistry registry = raftRpcFactory.getMarshallerRegistry();
//        registry.registerResponseInstance(CheckArgs4AddRequest.class.getName(),
//                CheckArgs4AddResponse.getDefaultInstance());

        if ("com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory".equals(RpcFactoryHelper.rpcFactory().getClass()
                .getName())) {
            logger.info("aaaaaa");
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(CheckArgs4AddRequest.class.getName(),
                    CheckArgs4AddRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(CheckArgs4AddResponse.class.getName(),
                    CheckArgs4AddResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(GetDataNodes4AddRequest.class.getName(),
                    GetDataNodes4AddRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(GetDataNodes4AddResponse.class.getName(),
                    GetDataNodes4AddResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(Callback4AddRequest.class.getName(),
                    Callback4AddRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(Callback4AddResponse.class.getName(),
                    Callback4AddResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(CheckArgs4GetRequest.class.getName(),
                    CheckArgs4GetRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(CheckArgs4GetResponse.class.getName(),
                    CheckArgs4GetResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(GetDataNodes4GetRequest.class.getName(),
                    GetDataNodes4GetRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(GetDataNodes4GetResponse.class.getName(),
                    GetDataNodes4GetResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(HeartbeatRequest.class.getName(),
                    HeartbeatRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(HeartbeatResponse.class.getName(),
                    HeartbeatResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(ListRequest.class.getName(),
                    ListRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(ListResponse.class.getName(),
                    ListResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(MkdirRequest.class.getName(),
                    MkdirRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(MkdirResponse.class.getName(),
                    MkdirResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(MoveRequest.class.getName(),
                    MoveRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(MoveResponse.class.getName(),
                    MoveResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(RemoveRequest.class.getName(),
                    RemoveRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(RemoveResponse.class.getName(),
                    RemoveResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(RenameRequest.class.getName(),
                    RenameRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(RenameResponse.class.getName(),
                    RenameResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(StatRequest.class.getName(),
                    StatRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(StatResponse.class.getName(),
                    StatResponse.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(DNRegisterRequest.class.getName(),
                    DNRegisterRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(DNRegisterResponse.class.getName(),
                    DNRegisterResponse.getDefaultInstance());

            try {
                Class<?> clazz = Class.forName("com.alipay.sofa.jraft.rpc.impl.MarshallerHelper");
                Method registerRespInstance = clazz.getMethod("registerRespInstance", String.class, Message.class);
                registerRespInstance.invoke(null, CheckArgs4AddRequest.class.getName(),
                        CheckArgs4AddResponse.getDefaultInstance());
                registerRespInstance.invoke(null, GetDataNodes4AddRequest.class.getName(),
                        GetDataNodes4AddResponse.getDefaultInstance());
                registerRespInstance.invoke(null, Callback4AddRequest.class.getName(),
                        Callback4AddResponse.getDefaultInstance());
                registerRespInstance.invoke(null, CheckArgs4GetRequest.class.getName(),
                        CheckArgs4GetResponse.getDefaultInstance());
                registerRespInstance.invoke(null, GetDataNodes4GetRequest.class.getName(),
                        GetDataNodes4GetResponse.getDefaultInstance());
                registerRespInstance.invoke(null, HeartbeatRequest.class.getName(),
                        HeartbeatResponse.getDefaultInstance());
                registerRespInstance.invoke(null, ListRequest.class.getName(),
                        ListResponse.getDefaultInstance());
                registerRespInstance.invoke(null, MkdirRequest.class.getName(),
                        MkdirResponse.getDefaultInstance());
                registerRespInstance.invoke(null, MoveRequest.class.getName(),
                        MoveResponse.getDefaultInstance());
                registerRespInstance.invoke(null, RemoveRequest.class.getName(),
                        RemoveResponse.getDefaultInstance());
                registerRespInstance.invoke(null, RenameRequest.class.getName(),
                        RenameResponse.getDefaultInstance());
                registerRespInstance.invoke(null, StatRequest.class.getName(),
                        StatResponse.getDefaultInstance());
                registerRespInstance.invoke(null, DNRegisterRequest.class.getName(),
                        DNRegisterResponse.getDefaultInstance());
            } catch (Exception e) {
                logger.error("Failed to init grpc server", e);
            }
        }
    }

    public static void setRpcServer(RpcServer rpcServer) {
        MasterGrpcHelper.rpcServer = rpcServer;
    }

    public static void blockUntilShutdown() {
        if (rpcServer == null) {
            return;
        }
        if ("com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory".equals(RpcFactoryHelper.rpcFactory().getClass()
                .getName())) {
            try {
                Method getServer = rpcServer.getClass().getMethod("getServer");
                Object grpcServer = getServer.invoke(rpcServer);

                Method shutdown = grpcServer.getClass().getMethod("shutdown");
                Method awaitTerminationLimit = grpcServer.getClass().getMethod("awaitTermination", long.class,
                        TimeUnit.class);

                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        try {
                            shutdown.invoke(grpcServer);
                            awaitTerminationLimit.invoke(grpcServer, 30, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                            e.printStackTrace(System.err);
                        }
                    }
                });
                Method awaitTermination = grpcServer.getClass().getMethod("awaitTermination");
                awaitTermination.invoke(grpcServer);
            } catch (Exception e) {
                logger.error("Failed to block grpc server", e);
            }
        }
    }
}
