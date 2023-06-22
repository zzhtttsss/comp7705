package org.comp7705.grpc;

import com.alipay.sofa.jraft.rpc.RpcServer;

import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;
import org.comp7705.protocol.definition.CheckArgs4AddRequest;
import org.comp7705.protocol.definition.CheckArgs4AddResponse;
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
            logger.info("aaaa");
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(CheckArgs4AddRequest.class.getName(),
                    CheckArgs4AddRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(CheckArgs4AddResponse.class.getName(),
                    CheckArgs4AddResponse.getDefaultInstance());

            try {
                Class<?> clazz = Class.forName("com.alipay.sofa.jraft.rpc.impl.MarshallerHelper");
                Method registerRespInstance = clazz.getMethod("registerRespInstance", String.class, Message.class);
                registerRespInstance.invoke(null, CheckArgs4AddRequest.class.getName(),
                        CheckArgs4AddResponse.getDefaultInstance());
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
