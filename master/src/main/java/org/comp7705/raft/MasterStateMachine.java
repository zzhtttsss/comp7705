package org.comp7705.raft;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.google.protobuf.Message;
import org.comp7705.Master;
import org.comp7705.operation.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

public class MasterStateMachine  extends StateMachineAdapter {
    private static final Logger logger = LoggerFactory.getLogger(MasterStateMachine.class);

    private static final ThreadPoolExecutor executor   = ThreadPoolUtil
            .newBuilder()
            .poolName("MASTER_RAFT_EXECUTOR")
            .enableMetric(true)
            .coreThreads(3)
            .maximumThreads(5)
            .keepAliveSeconds(60L)
            .workQueue(new SynchronousQueue<>())
            .threadFactory(
                    new NamedThreadFactory("Master-Raft-Executor-", true)).build();



    /**
     * Counter value
     */
    private final Master master      = Master.MASTER;
    /**
     * Leader term
     */
    private final AtomicLong          leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void onApply(Iterator iterator) {
        while (iterator.hasNext()) {
            Operation operation;
            RequestProcessClosure closure = null;
            if (iterator.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (RequestProcessClosure) iterator.done();
                operation = closure.getOperation();
            } else {
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iterator.getData();
                operation = deserializeOperation(data);
                // follower ignore read operation
//                if (operation != null && operation.isReadOp()) {
//                    iter.next();
//                    continue;
//                }
            }

            if (operation != null) {
                try {
                    Message response = operation.apply();
                    if (closure != null) {
                        closure.sendResponse(response);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            iterator.next();
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        executor.submit(() -> {
//            final MasterSnapshotFile snapshot = new MasterSnapshotFile(writer.getPath() + File.separator + "data");
//            if (snapshot.save(currVal)) {
//                if (writer.addFile("data")) {
//                    done.run(Status.OK());
//                } else {
//                    done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
//                }
//            } else {
//                done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
//            }
        });
    }

    @Override
    public void onError(final RaftException e) {
        logger.error("Raft error: {}", e, e);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
//        if (isLeader()) {
//            logger.warn("Leader is not supposed to load snapshot");
//            return false;
//        }
//        if (reader.getFileMeta("data") == null) {
//            logger.error("Fail to find data file in {}", reader.getPath());
//            return false;
//        }
//        final MasterSnapshotFile snapshot = new MasterSnapshotFile(reader.getPath() + File.separator + "data");
//        try {
//            this.value.set(snapshot.load());
//            return true;
//        } catch (final IOException e) {
//            logger.error("Fail to load snapshot from {}", snapshot.getPath());
//            return false;
//        }
        return true;

    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

    private Operation deserializeOperation(final ByteBuffer data) {
        Operation operation = null;
        try {
            // bytearray to object

            byte[] bytes = new byte[data.remaining()];
            data.get(bytes, 0, bytes.length);
            ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
            ObjectInputStream oi = new ObjectInputStream(bi);
            operation = (Operation) oi.readObject();
            bi.close();
            oi.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return operation;
    }
}
