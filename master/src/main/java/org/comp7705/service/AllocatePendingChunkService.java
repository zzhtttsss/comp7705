package org.comp7705.service;

import com.alipay.sofa.jraft.entity.Task;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.Master;
import org.comp7705.MasterServer;
import org.comp7705.operation.AllocateChunksOperation;
import org.comp7705.thread.ServiceThread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.comp7705.raft.MasterRequestProcessor.serializeOperation;

@Slf4j
public class AllocatePendingChunkService extends ServiceThread {

    private final MasterServer masterServer;

    private final Master master = Master.MASTER;

    public AllocatePendingChunkService(MasterServer masterServer) {
        this.masterServer = masterServer;
    }

    @Override
    public String getServiceName() {
        return AllocatePendingChunkService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started.", this.getServiceName());
        if (master.getChunkManager().getPendingChunkQueue().size() != 0) {
            try {
                batchAllocateChunks();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long lastFlushTime = System.currentTimeMillis();
        while (!this.stopped) {
            if (System.currentTimeMillis() - lastFlushTime > 10000 ||
                    master.getChunkManager().getPendingChunkQueue().size() > 16) {
                try {
                    batchAllocateChunks();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                lastFlushTime = System.currentTimeMillis();
            }
        }
        log.info("{} service end.", this.getServiceName());
    }

    private void batchAllocateChunks() throws IOException {
        log.info("Start to allocate a batch of chunks.");
        if (master.getChunkManager().getPendingChunkQueue().size() != 0) {
            List<String> batchChunkIds = master.getChunkManager().getPendingChunks();
            List<String> chunkIds = master.getChunkManager().batchFilterChunk(batchChunkIds);
            List<String> dataNodeIds = master.getDataNodeManager().getAliveDataNodeIds();
            boolean[][] isStore = master.getChunkManager().getStoreState(chunkIds, dataNodeIds);
            // Todo DataNode num is less than replicate num or other similar situation
            //  so that a Chunk can not find a DataNode to store.
            int[] receiverPlan = allocateChunksDFS(chunkIds.size(), dataNodeIds.size(), isStore);
            for (int i = 0; i < isStore.length; i++) {
                for (int j = 0; j < isStore[0].length; j++) {
                    isStore[i][j] = !isStore[i][j];
                }
            }
            int[] senderPlan = allocateChunksDFS(chunkIds.size(), dataNodeIds.size(), isStore);
            log.debug("Receiver plan is {}", receiverPlan);
            log.debug("Sender plan is {}", senderPlan);
            AllocateChunksOperation operation = new AllocateChunksOperation(UUID.randomUUID().toString(), senderPlan,
                    receiverPlan, chunkIds, dataNodeIds, batchChunkIds.size());
            Task task = new Task();
            task.setData(ByteBuffer.wrap(serializeOperation(operation)));
            task.setDone(status -> {});
            masterServer.getNode().apply(task);
        }
        log.info("Success to allocate a batch of chunks.");
    }

    private int[] allocateChunksDFS(int chunkNum, int dataNodeNum, boolean[][] isStore) {
        List<List<Integer>> currentResult = new ArrayList<>();
        for (int i = 0; i < dataNodeNum; i++) {
            currentResult.add(new ArrayList<>());
        }
        int[] result = new int[chunkNum];
        int[] minValue = new int[1];
        minValue[0] = Integer.MAX_VALUE;
        int avg = (int) Math.ceil((double) chunkNum / dataNodeNum);
        int bestVariance = calBestVariance(chunkNum, dataNodeNum, avg);
        for (int i = 0; i < dataNodeNum; i++) {
            if (dfs(chunkNum, dataNodeNum, 0, i, currentResult, isStore, result, minValue, avg,
                    bestVariance)) {
                break;
            }
        }
        return result;
    }

    private int calBestVariance(int chunkNum, int dataNodeNum, int avg) {
        if (avg * dataNodeNum == chunkNum) {
            return 0;
        }
        return chunkNum - (avg - 1) * dataNodeNum;
    }

    private boolean dfs(int chunkNum, int dataNodeNum, int chunkIndex, int dnIndex, List<List<Integer>> currentResult,
                        boolean[][] isStore, int[] result, int[] minValue, int avg, int bestVariance) {
        if (chunkIndex == chunkNum) {
            int currentValue = 0;
            for (int i = 0; i < dataNodeNum; i++) {
                currentValue += Math.pow(currentResult.get(i).size() - avg, 2);
            }
            if (currentValue < minValue[0]) {
                minValue[0] = currentValue;
                for (int j = 0; j < dataNodeNum; j++) {
                    for (int k = 0; k < currentResult.get(j).size(); k++) {
                        result[currentResult.get(j).get(k)] = j;
                    }
                }
            }
            // If the best plan has been found, just stop dfs and return the result.
            return currentValue == bestVariance;
        }
        currentResult.get(dnIndex).add(chunkIndex);
        for (int i = 0; i < dataNodeNum; i++) {
            if (isStore[chunkIndex][dnIndex]) {
                continue;
            }
            isStore[chunkIndex][dnIndex] = true;
            boolean isBest = dfs(chunkNum, dataNodeNum, chunkIndex + 1, i, currentResult, isStore, result,
                    minValue, avg, bestVariance);
            isStore[chunkIndex][dnIndex] = false;
            if (isBest) {
                return true;
            }
        }
        currentResult.get(dnIndex).remove(currentResult.get(dnIndex).size() - 1);
        return false;
    }

}
