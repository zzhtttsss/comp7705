package org.comp7705.client.services.impl;

import lombok.extern.slf4j.Slf4j;
import org.comp7705.client.entity.ChunkAddResult;
import org.comp7705.client.services.ChunkClient;
import org.comp7705.client.services.ClientService;
import org.comp7705.client.services.MasterClient;
import org.comp7705.client.utils.*;
import org.comp7705.protocol.definition.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;


@Slf4j
public class ClientServiceImpl implements ClientService {
    private final MasterClient masterClient = MasterClient.getInstance();
    private final ChunkClient chunkClient = ChunkClient.getInstance();

    private final Random shuffleRandom = new Random();

    @Override
    public void mkdir(String path, String name) {
        masterClient.mkdir(path, name);
    }

    @Override
    public void list(String path, boolean isLatest) {
        masterClient.list(path, isLatest);
    }

    @Override
    public void add(String src, String des) {
        File file = new File(src);
        if (!file.exists() || !file.isFile()) {
            log.error("No such file {}", src);
            System.out.printf("\033[1;31mNo such file %s\033[0m\n", src);
            return;
        }
        CheckArgs4AddResponse addInfo = masterClient.checkArgs4Add(file.getName(), des, file.length());
        if (addInfo == null) { return; }
        ProgressBar bar = ProgressBar.build(0, 100 / addInfo.getChunkNum(), "Uploading");
        GetDataNodes4AddResponse dataNodeInfo = masterClient.getDataNodes4Add(addInfo.getFileNodeId(), addInfo.getChunkNum());
        if (dataNodeInfo == null) { return; }
        ArrayList<ChunkAddResult> resultList = new ArrayList<>();

        try (
                InputStream in = new FileInputStream(file);
                BufferedInputStream bin = new BufferedInputStream(in);
        ) {
            byte[] buffer = new byte[Const.ChunkSize];
            for (int i = 0; i < addInfo.getChunkNum(); i++) {
                int k = shuffleRandom.nextInt(dataNodeInfo.getDataNodeIdsCount());
                List<String> dataNodeIds = ProtobufUtil.byteStrList2StrList(dataNodeInfo.getDataNodeIds(i).getItemsList().asByteStringList());
                List<String> dataNodeAdds = ProtobufUtil.byteStrList2StrList(dataNodeInfo.getDataNodeAdds(i).getItemsList().asByteStringList());
                Collections.swap(dataNodeAdds, 0, k);
                Collections.swap(dataNodeIds, 0, k);
                String chunkId = addInfo.getFileNodeId() + "_" + i;
                int chunkSize = bin.read(buffer);
                List<String> checkSums = StringUtil.getInstance().getCheckSums(buffer, Const.MB);
                List<TransferChunkResponse> responses = chunkClient.addFile(chunkId, dataNodeAdds, chunkSize, checkSums, buffer);
                resultList.add(ChunkAddResult.fromTransferChunkResponse(responses.get(responses.size() - 1), dataNodeAdds, chunkId));
                bar.step();
            }
            int failNum = 0;
            ArrayList<ChunkInfo4Add> infos = new ArrayList<>();
            ArrayList<String> failChunkIds = new ArrayList<>();
            for (ChunkAddResult result : resultList) {
                if (result.successNodes.size() == 0) {
                    failNum++;
                    failChunkIds.add(result.chunkId);
                } else {
                    infos.add(result.toChunkInfo4Add());
                }
            }
            Callback4AddResponse callBackInfo = masterClient.callBack4Add(addInfo.getFileNodeId(), des, infos, failChunkIds);
            if (callBackInfo == null) { return; }
            if (failNum > 0) {
                log.error("Fail to add {} chunks", failNum);
                System.out.printf("\033[1;31m\"Fail to add %s chunks\033[0m\n", failNum);
            }
        } catch (FileNotFoundException e) {
            log.error("No such file {}", src);
            System.out.printf("\033[1;31mNo such file %s\033[0m\n", src);
        } catch (IOException e) {
            log.error("IO Exception {}", e.toString());
            System.out.printf("\033[1;31mIO Exception %s\033[0m\n", e);
        }
    }

    @Override
    public void get(String src, String des) {
        CheckArgs4GetResponse getInfo = masterClient.checkArgs4Get(src);
        if (getInfo == null) { return; }
        try (OutputStream out = Files.newOutputStream(Paths.get(des));
             BufferedOutputStream bout = new BufferedOutputStream(out)) {
            for (int i = 0; i < getInfo.getChunkNum(); i++){
                GetDataNodes4GetResponse dataNodesResponse = masterClient.getDataNodes4Get(getInfo.getFileNodeId(), i);
                if (dataNodesResponse == null) { return; }
                String chunkId = getInfo.getFileNodeId() + "_" + i;
                List<String> nodeAddresses = ProtobufUtil.byteStrList2StrList(dataNodesResponse.getDataNodeAddrsList().asByteStringList());
                boolean isChunkSuccess = false;
                for (String nodeAddress: nodeAddresses) {
                    if (chunkClient.getFile(chunkId, nodeAddress, bout)) {
                        isChunkSuccess = true;
                        break;
                    }
                }
                if (!isChunkSuccess) {
                    break;
                }
            }
        }  catch (IOException e) {
            log.error(e.toString());
            System.out.printf("\033[1;31mFailed to write %s because of %s\033[0m\n", des, e);
        }

    }

    @Override
    public void move(String src, String des) {
        masterClient.move(src, des);
    }

    @Override
    public void remove(String src) { masterClient.remove(src); }

    @Override
    public void rename(String src, String des) { masterClient.rename(src, des); }

    @Override
    public void stat(String src, boolean isLatest) { masterClient.stat(src, isLatest); }

    @Override
    public void close() {

    }

}