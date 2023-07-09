package org.comp7705.client.services.impl;

import com.alipay.sofa.jraft.error.RemotingException;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.client.entity.ChunkAddResult;
import org.comp7705.client.services.ChunkClient;
import org.comp7705.client.services.ClientService;
import org.comp7705.client.services.MasterClient;
import org.comp7705.client.utils.*;
import org.comp7705.constant.Const;
import org.comp7705.protocol.definition.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;
import java.util.concurrent.TimeoutException;


@Slf4j
public class ClientServiceImpl implements ClientService {
    private final MasterClient masterClient = MasterClient.masterClient;
    private final ChunkClient chunkClient = ChunkClient.getInstance();

    private final Random shuffleRandom = new Random();

    @Override
    public void mkdir(String path, String name) {
        try {
            masterClient.mkdir(path, name);
        } catch (RemotingException | TimeoutException e) {
            log.error("Fail to mkdir.", e);
        } catch (InterruptedException ignored) {

        }
    }

    @Override
    public void list(String path, boolean isLatest) {
        try {
            masterClient.list(path, isLatest);
        } catch (RemotingException | TimeoutException e) {
            log.error("Fail to list.", e);
        } catch (InterruptedException ignored) {

        }
    }

    @Override
    public void add(String src, String des) {
        File file = new File(src);
        if (!file.exists() || !file.isFile()) {
            log.error("No such file {}", src);
            System.out.printf("\033[1;31mNo such file %s\033[0m\n", src);
            return;
        }
        CheckArgs4AddResponse checkArgs4AddResponse = null;
        log.info("filename: {}", PathUtil.getCurrentName(des));
        log.info("parent path: {}", PathUtil.getParentPath(des));
        try {
            checkArgs4AddResponse = masterClient.checkArgs4Add(PathUtil.getCurrentName(des),
                    PathUtil.getParentPath(des), file.length());
        } catch (RemotingException | TimeoutException e) {
            log.error("fail to check args for add.", e);
        } catch (InterruptedException e) {
            return;
        }
        if (checkArgs4AddResponse == null) {
            return;
        }
        ProgressBar bar = ProgressBar.build(0, 100 / checkArgs4AddResponse.getChunkNum(), "Uploading");
        GetDataNodes4AddResponse getDataNodes4AddResponse = null;
        try {
            getDataNodes4AddResponse = masterClient.getDataNodes4Add(checkArgs4AddResponse.getFileNodeId(),
                    checkArgs4AddResponse.getChunkNum());
        } catch (RemotingException | TimeoutException e) {
            log.error("Fail to get data nodes for add.", e);
        } catch (InterruptedException e) {
            return;
        }
        if (getDataNodes4AddResponse == null) {
            return;
        }
        ArrayList<ChunkAddResult> resultList = new ArrayList<>();

        try (
                InputStream in = new FileInputStream(file);
                BufferedInputStream bin = new BufferedInputStream(in);
        ) {
            byte[] buffer = new byte[Const.ChunkSize];
            int index = 0;
            for (int i = 0; i < checkArgs4AddResponse.getChunkNum(); i++) {
                if (index + Const.ChunkSize > file.length()) {
                    buffer = new byte[(int) (file.length() - index)];
                }
                int k = shuffleRandom.nextInt(getDataNodes4AddResponse.getDataNodeIdsCount());
                List<String> dataNodeIds = ProtobufUtil.byteStrList2StrList(
                        getDataNodes4AddResponse.getDataNodeIds(i).getItemsList().asByteStringList());
                List<String> dataNodeAdds = ProtobufUtil.byteStrList2StrList(
                        getDataNodes4AddResponse.getDataNodeAdds(i).getItemsList().asByteStringList());
//                Collections.swap(dataNodeAdds, 0, k);
//                Collections.swap(dataNodeIds, 0, k);
                String chunkId = checkArgs4AddResponse.getFileNodeId() + "_" + i;
                int chunkSize = bin.read(buffer);
                List<String> checkSums = StringUtil.getInstance().getCheckSums(buffer, Const.MB);
                List<TransferChunkResponse> responses = chunkClient.addFile(chunkId, dataNodeAdds,
                        chunkSize, checkSums, buffer);
                List<String> failNodes = ProtobufUtil.byteStrList2StrList(responses.get(responses.size() - 1)
                        .getFailAddsList().asByteStringList());
                List<String> failNodeIds = new ArrayList<>();
                for (String failNode : failNodes) {
                    for (String address : dataNodeAdds) {
                        if (address.equals(failNode)) {
                            failNodeIds.add(dataNodeIds.get(dataNodeAdds.indexOf(address)));
                        }
                    }
                }
                ArrayList<String> successNodeIds = new ArrayList<>(dataNodeIds);
                successNodeIds.removeAll(failNodeIds);
                ChunkAddResult chunkAddResult = new ChunkAddResult(chunkId, successNodeIds, failNodeIds);
                resultList.add(chunkAddResult);
                bar.step();
                index += Const.ChunkSize;
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
            Callback4AddResponse callBackInfo = null;
            try {
                callBackInfo = masterClient.callBack4Add(checkArgs4AddResponse.getFileNodeId(), des, infos, failChunkIds);
            } catch (RemotingException | TimeoutException e) {
                log.error("Fail to callback for add.", e);
            } catch (InterruptedException e) {
                return;
            }
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
        CheckArgs4GetResponse getInfo = null;
        try {
            getInfo = masterClient.checkArgs4Get(src);
        } catch (RemotingException | TimeoutException e) {
            log.error("Fail to check args for get.", e);
        } catch (InterruptedException e) {
            return;
        }
        if (getInfo == null) { return; }
        try (OutputStream out = Files.newOutputStream(Paths.get(des));
             BufferedOutputStream bout = new BufferedOutputStream(out)) {
            for (int i = 0; i < getInfo.getChunkNum(); i++){
                GetDataNodes4GetResponse dataNodesResponse = null;
                try {
                    dataNodesResponse = masterClient.getDataNodes4Get(getInfo.getFileNodeId(), i);
                } catch (RemotingException | TimeoutException e) {
                    log.error("Fail to get data nodes for get.", e);
                } catch (InterruptedException e) {
                    return;
                }
                log.info("data nodes response: {}", dataNodesResponse);
                if (dataNodesResponse == null) { return; }
                String chunkId = getInfo.getFileNodeId() + "_" + i;
                List<String> nodeAddresses = ProtobufUtil.byteStrList2StrList(dataNodesResponse
                        .getDataNodeAddrsList().asByteStringList());
                boolean isChunkSuccess = false;
                for (String nodeAddress: nodeAddresses) {
                    log.info("node address: {}", nodeAddress);
                    if (chunkClient.getFile(chunkId, nodeAddress, bout)) {
                        isChunkSuccess = true;
                        break;
                    }
                }
                if (!isChunkSuccess) {
                    throw new IOException("Fail to get chunk " + chunkId);
                }
            }
        }  catch (IOException e) {
            log.error("Fail to write {}", des, e);
            System.out.printf("\033[1;31mFailed to write %s because of %s\033[0m\n", des, e);
        }

    }

    @Override
    public void move(String src, String des) {
        try {
            masterClient.move(src, des);
        } catch (RemotingException | TimeoutException e) {
            log.error("Fail to move {} to {}", src, des, e);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void remove(String src) {
        try {
            masterClient.remove(src);
        } catch (RemotingException | TimeoutException e) {
            log.error("Fail to remove {}", src, e);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void rename(String src, String des) {
        try {
            masterClient.rename(src, des);
        } catch (RemotingException | TimeoutException e) {
            log.error("Fail to rename {} to {}", src, des, e);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void stat(String src, boolean isLatest) {
        try {
            masterClient.stat(src, isLatest);
        } catch (RemotingException | TimeoutException e) {
            log.error("Fail to get the state of {}", src, e);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void close() {

    }

}
