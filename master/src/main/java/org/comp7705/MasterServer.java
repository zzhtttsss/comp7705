package org.comp7705;

import io.grpc.stub.StreamObserver;
import org.comp7705.protocol.definition.*;
import org.comp7705.protocol.service.MasterServiceGrpc;

public class MasterServer extends MasterServiceGrpc.MasterServiceImplBase {

    @Override
    public void joinCluster(JoinClusterRequest request, StreamObserver<JoinClusterResponse> responseObserver) {

    }

    @Override
    public void register(DNRegisterRequest request, StreamObserver<DNRegisterResponse> responseObserver) {
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
    }

    @Override
    public void checkArgs4Add(CheckArgs4AddRequest request, StreamObserver<CheckArgs4AddResponse> responseObserver) {
    }

    @Override
    public void getDataNodes4Add(GetDataNodes4AddRequest request, StreamObserver<GetDataNodes4AddResponse> responseObserver) {
    }

    @Override
    public void callback4Add(Callback4AddRequest request, StreamObserver<Callback4AddResponse> responseObserver) {
    }

    @Override
    public void checkArgs4Get(CheckArgs4GetRequest request, StreamObserver<CheckArgs4GetResponse> responseObserver) {
    }

    @Override
    public void getDataNodes4Get(GetDataNodes4GetRequest request, StreamObserver<GetDataNodes4GetResponse> responseObserver) {
    }

    @Override
    public void releaseLease4Get(ReleaseLease4GetRequest request, StreamObserver<ReleaseLease4GetResponse> responseObserver) {
    }

    @Override
    public void list(ListRequest request, StreamObserver<ListResponse> responseObserver) {
    }

    @Override
    public void mkdir(MkDirRequest request, StreamObserver<MkDirResponse> responseObserver) {
    }

    @Override
    public void move(MoveRequest request, StreamObserver<MoveResponse> responseObserver) {
    }

    @Override
    public void stat(StatRequest request, StreamObserver<StatResponse> responseObserver) {
    }

    @Override
    public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    }

    @Override
    public void rename(RenameRequest request, StreamObserver<RenameResponse> responseObserver) {
    }

    public void close() {

    }
}
