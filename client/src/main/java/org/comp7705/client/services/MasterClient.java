package org.comp7705.client.services;

import com.google.protobuf.BlockingRpcChannel;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.client.utils.DebugUtil;
import org.comp7705.protocol.definition.*;
import org.comp7705.protocol.service.MasterServiceGrpc;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MasterClient {
    private static final String host = "localhost";
    private static final int port = 9000;
    private final ManagedChannel channel;
    private static MasterClient instance = null;

    private static final int TIME_OUT = 5;

    private MasterClient() {
    channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
}

    public void mkdir(String path, String dirName) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        MkDirRequest request = MkDirRequest
                .newBuilder()
                .setPath(path)
                .setDirName(dirName)
                .build();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.mkdir(request, new StreamObserver<MkDirResponse>() {
            @Override
            public void onNext(MkDirResponse response) {
                System.out.printf("%s is Successfully created\n", path);
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to create the %s because of %s\033[0m\n", path, throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mThe creation of dir %s is interrupted by %s\033[0m\n", path, e);
        }
    }

    public void move(String src, String des) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        MoveRequest request = MoveRequest
                .newBuilder()
                .setSourcePath(src)
                .setTargetPath(des)
                .build();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.move(request, new StreamObserver<MoveResponse>() {
            @Override
            public void onNext(MoveResponse response) {
                System.out.printf("%s is moved to %s\n", src, des);
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to move the %s because of %s\033[0m\n", src, throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mThe move operation is interrupted by %s\033[0m\n", e);
        }
    }

    public void remove(String src) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        RemoveRequest request = RemoveRequest
                .newBuilder()
                .setPath(src)
                .build();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.remove(request, new StreamObserver<RemoveResponse>() {
            @Override
            public void onNext(RemoveResponse response) {
                System.out.printf("%s is removed\n", src);
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to remove %s because of %s\033[0m\n", src, throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mThe remove operation is interrupted by %s\033[0m\n", e);
        }
    }

    public void rename(String src, String des) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        RenameRequest request = RenameRequest
                .newBuilder()
                .setPath(src)
                .setNewName(des)
                .build();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.rename(request, new StreamObserver<RenameResponse>() {
            @Override
            public void onNext(RenameResponse response) {
                System.out.printf("%s is renamed to %s\n", src, des);
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to rename %s because of %s\033[0m\n", src, throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mThe rename operation is interrupted by %s\033[0m\n", e);
        }
    }

    public void stat(String src, boolean isLatest) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        StatRequest request = StatRequest
                .newBuilder()
                .setPath(src)
                .build();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.stat(request, new StreamObserver<StatResponse>() {
            @Override
            public void onNext(StatResponse response) {
                if (response.getIsFile()) {
                    System.out.printf("Filename: %s Size: %d\n", response.getFileName(), response.getSize());
                } else {
                    System.out.printf("Directory: %s\n", response.getFileName());
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to stat %s because of %s\033[0m\n", src, throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mThe stat operation is interrupted by %s\033[0m\n", e);
        }
    }

    public void list(String path, boolean isLatest) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        ListRequest request = ListRequest
                .newBuilder()
                .setPath(path)
                .setIsLatest(isLatest)
                .build();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.list(request, new StreamObserver<ListResponse>() {
            @Override
            public void onNext(ListResponse response) {
                List<FileInfo> fileInfos = response.getFilesList();
                for (FileInfo info : fileInfos) {
                    if (info.getIsFile()) {
                        System.out.printf("\033[4;37m%s\033[0m ", info.getFileName());
                    } else {
                        System.out.printf("\033[4;36m%s\033[0m ", info.getFileName());
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to list the %s because of %s\033[0m\n", path, throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mThe list dir %s is interrupted by %s\033[0m\n", path, e);
        }
    }

    public CheckArgs4AddResponse checkArgs4Add(String filename, String path, long filesize) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        CheckArgs4AddRequest request = CheckArgs4AddRequest
                .newBuilder()
                .setFileName(filename)
                .setPath(path)
                .setSize(filesize)
                .build();
        final CheckArgs4AddResponse[] addResponse = new CheckArgs4AddResponse[1];
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.checkArgs4Add(request, new StreamObserver<CheckArgs4AddResponse>() {
            @Override
            public void onNext(CheckArgs4AddResponse response) {
                addResponse[0] = response;
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to create file %s because of %s\033[0m\n", filename, throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mThe creation of file %s is interrupted by %s\033[0m\n", filename, e);
        }
        return addResponse[0];
    }

    public GetDataNodes4AddResponse getDataNodes4Add(String fileNodeId, int chunkNum) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        GetDataNodes4AddRequest request = GetDataNodes4AddRequest
                .newBuilder()
                .setFileNodeId(fileNodeId)
                .setChunkNum(chunkNum)
                .build();
        final GetDataNodes4AddResponse[] addResponse = new GetDataNodes4AddResponse[1];
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.getDataNodes4Add(request, new StreamObserver<GetDataNodes4AddResponse>() {
            @Override
            public void onNext(GetDataNodes4AddResponse response) {
                addResponse[0] = response;
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to run %s because of %s\033[0m\n", DebugUtil.getMethodName(), throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mFailed to run %s because of %s\033[0m\n", DebugUtil.getMethodName(), e);
        }
        return addResponse[0];
    }

    public Callback4AddResponse callBack4Add(String fileNodeId, String filepath, List<ChunkInfo4Add> infos, List<String> failChunkIds) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        Callback4AddRequest.Builder requestBuilder = Callback4AddRequest
                .newBuilder()
                .setFileNodeId(fileNodeId)
                .setFilePath(filepath);
        for (int i = 0; i < infos.size(); i++) {
            requestBuilder.setInfos(i, infos.get(i));
        }
        for (int i = 0; i < failChunkIds.size(); i++) {
            requestBuilder.setFailChunkIds(i, failChunkIds.get(i));
        }

        final Callback4AddResponse[] addResponse = new Callback4AddResponse[1];
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.callback4Add(requestBuilder.build(), new StreamObserver<Callback4AddResponse>() {
            @Override
            public void onNext(Callback4AddResponse response) {
                addResponse[0] = response;
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to run %s because of %s\033[0m\n", DebugUtil.getMethodName(), throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mFailed to run %s because of %s\033[0m\n", DebugUtil.getMethodName(), e);
        }
        return addResponse[0];
    }

    public CheckArgs4GetResponse checkArgs4Get(String path) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        CheckArgs4GetRequest request = CheckArgs4GetRequest
                .newBuilder()
                .setPath(path)
                .build();
        final CheckArgs4GetResponse[] getResponse = new CheckArgs4GetResponse[1];
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.checkArgs4Get(request, new StreamObserver<CheckArgs4GetResponse>() {
            @Override
            public void onNext(CheckArgs4GetResponse response) {
                getResponse[0] = response;
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to get file %s because of %s\033[0m\n", path, throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mThe get of file %s is interrupted by %s\033[0m\n", path, e);
        }
        return getResponse[0];
    }

    public GetDataNodes4GetResponse getDataNodes4Get(String fileNodeId, int chunkIndex) {
        MasterServiceGrpc.MasterServiceStub stub = MasterServiceGrpc.newStub(channel)
                .withDeadline(Deadline.after(TIME_OUT, TimeUnit.SECONDS));
        GetDataNodes4GetRequest request = GetDataNodes4GetRequest
                .newBuilder()
                .setFileNodeId(fileNodeId)
                .setChunkIndex(chunkIndex)
                .build();
        final GetDataNodes4GetResponse[] getResponse = new GetDataNodes4GetResponse[1];
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stub.getDataNodes4Get(request, new StreamObserver<GetDataNodes4GetResponse>() {
            @Override
            public void onNext(GetDataNodes4GetResponse response) {
                getResponse[0] = response;
            }

            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.toString());
                System.out.printf("\033[1;31mFailed to run %s because of %s\033[0m\n", DebugUtil.getMethodName(), throwable);
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mFailed to run %s because of %s\033[0m\n", DebugUtil.getMethodName(), e);
        }
        return getResponse[0];
    }

    @Synchronized
    public static MasterClient getInstance() {
        if (instance == null) {
            instance = new MasterClient();
        }
        return instance;
    }

    @Synchronized
    public void close() {
        channel.shutdown();
        instance = null;
    }
}
