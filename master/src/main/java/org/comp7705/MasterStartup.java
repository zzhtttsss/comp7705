package org.comp7705;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.grpc.ContextInterceptor;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.comp7705.Master.MASTER;
import static org.comp7705.MasterConfig.MASTER_CONFIG;


@Slf4j
public class MasterStartup {
    private static Server server;
    private static MasterServer masterServer;

    public static void start() throws IOException {
        // read config first
        MASTER.init();
        // The port on which the server should run
        int port = MASTER_CONFIG.getMasterGrpcPort();
        masterServer = new MasterServer();
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(masterServer)
                .addService(ChannelzService.newInstance(100))
                .addService(ProtoReflectionService.newInstance())
                .intercept(new ContextInterceptor())
                .build()
                .start();

        log.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                log.warn("*** shutting down gRPC server since JVM is shutting down");
                try {
                    MasterStartup.stop();
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
                log.warn("*** server shut down");
            }
        });
    }

    public static void stop() throws InterruptedException {
        if (server != null) {
            masterServer.close();
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public static void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        MasterStartup.start();
        MasterStartup.blockUntilShutdown();
    }
}
