package comp7705.chunkserver.service.Impl;

import org.comp7705.constant.Const;
import org.comp7705.util.Util;
import comp7705.chunkserver.exception.ChecksumException;
import comp7705.chunkserver.exception.ListException;
import comp7705.chunkserver.service.FileService;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.protocol.definition.PieceOfChunk;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
@Slf4j
public class FileServiceImpl implements FileService {

    private final static String ROOT_PATH = Const.PATH + File.separator + "storage";
    private final static String STORAGE_PATH = ROOT_PATH + File.separator + "chunk";
    private final static String CHECKSUM_PATH = ROOT_PATH + File.separator + "checksum";

    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private Map<String, RandomAccessFile> chunk2ChunkFile;
    private Map<String, RandomAccessFile> chunk2ChecksumFile;

    public FileServiceImpl() {
        this.lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
        this.chunk2ChunkFile = new HashMap<>();
        this.chunk2ChecksumFile = new HashMap<>();
    }

    @Override
    public void storeChunk(PieceOfChunk pieceOfChunk, String chunkId, int chunkSize, String checksum) throws IOException {
        log.info("Storing a piece");
        String chunkName = STORAGE_PATH + File.separator + chunkId + "_incomplete";
        String checksumFileName = CHECKSUM_PATH + File.separator + chunkId;

        createFile(chunkName);
        createFile(checksumFileName);

        RandomAccessFile chunk;
        if (chunk2ChunkFile.containsKey(chunkId)) {
            chunk = chunk2ChunkFile.get(chunkId);
        } else {
            chunk = new RandomAccessFile(chunkName, "rw");
            chunk2ChunkFile.put(chunkId, chunk);
        }
        chunk.write(pieceOfChunk.getPiece().toByteArray());

        RandomAccessFile checksumFile;
        if (chunk2ChecksumFile.containsKey(chunkId)) {
            checksumFile = chunk2ChecksumFile.get(chunkId);
        } else {
            checksumFile = new RandomAccessFile(checksumFileName, "rw");
            chunk2ChecksumFile.put(chunkId, checksumFile);
        }
        checksumFile.writeInt(checksum.getBytes().length);
        checksumFile.write(checksum.getBytes());
    }

    @Override
    public void finishStoreChunk(String chunkId) throws IOException {
        RandomAccessFile checksumFile = chunk2ChecksumFile.get(chunkId);
        checksumFile.writeInt(-1);
        chunk2ChunkFile.remove(chunkId).close();
        chunk2ChecksumFile.remove(chunkId).close();
    }

    @Override
    public void renameChunk(String oldName, String newName) throws IOException {

        File file = new File(STORAGE_PATH + File.separator + oldName);

        File newFile = new File(STORAGE_PATH + File.separator + newName);
        file.renameTo(newFile);
//        newFile.createNewFile();
//        FileInputStream fileInputStream = new FileInputStream(file);
//        FileOutputStream fileOutputStream = new FileOutputStream(newFile);
//        int by;
//        while ((by=fileInputStream.read())!=-1){
//            fileOutputStream.write(by);
//        }
//        fileInputStream.close();
//        fileOutputStream.close();
//        if (!file.delete()) {
//            log.warn(file.getName() + " deletion fails");
//        }
    }

    @Override
    public byte[] readChunk(String chunkId) throws Exception {

        RandomAccessFile chunk = new RandomAccessFile(STORAGE_PATH + File.separator + chunkId + "_complete", "r");
        byte[] output = new byte[(int) chunk.length()];
        chunk.read(output);
        chunk.close();
        return output;
    }

    @Deprecated
    public void storeChunkWithBuffer(PieceOfChunk pieceOfChunk, String chunkId, int chunkSize, String checksum) throws IOException {

        try {
            writeLock.lock();
            log.info("Storing chunkId: " + chunkId);

            createFile(STORAGE_PATH + chunkId);
            createFile(CHECKSUM_PATH + chunkId);

            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(STORAGE_PATH + chunkId))) {
                bufferedWriter.write(pieceOfChunk.getPiece().toString());
            }

            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(CHECKSUM_PATH + chunkId))) {
                bufferedWriter.write(checksum);
            }
        } finally {
            writeLock.unlock();
        }

    }

    private void createFile(String path) throws IOException {
        File file;
        file = new File(path);
        if (file.exists()) {
            return;
        }
        File parent = file.getParentFile();

        if (!parent.exists()) {
            parent.mkdirs();
        }

        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
    }

    private File createDir(String path) throws IOException {
        File file;
        file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
        }
        return file;
    }

    @Deprecated
    public byte[] readChunkWithBuffer(String chunkId) throws Exception {

        log.info("Reading chunkId: " + chunkId);

        byte[] res;
        File file;
        file = new File(STORAGE_PATH + chunkId);
        char[] chunkContent = new char[(int) file.length()];
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(STORAGE_PATH + chunkId))) {
            bufferedReader.read(chunkContent);
        }
        file = new File(CHECKSUM_PATH + chunkId);
        char[] checksumContent = new char[(int) file.length()];
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(CHECKSUM_PATH + chunkId))) {
            bufferedReader.read(checksumContent);
        }

        if (!Util.checksum(chunkContent, checksumContent)) {
            throw new ChecksumException();
        }

        return String.valueOf(chunkContent).getBytes();
    }

    @Override
    public void deleteChunk(String chunkId) throws Exception {
        File file;

        file = new File(STORAGE_PATH + File.separator + chunkId + "_complete");
        if (file.exists()) {
            file.delete();
        }

        file = new File(STORAGE_PATH + File.separator + chunkId + "_incomplete");
        if (file.exists()) {
            file.delete();
        }

        file = new File(CHECKSUM_PATH + File.separator + chunkId);
        if (file.exists()) {
            file.delete();
        }
    }

    @Override
    public void deleteChunk(List<String> chunkIds) throws Exception {
        List<Exception> exceptions = new ArrayList<>();
        for (String chunkId : chunkIds) {
            try {
                deleteChunk(chunkId);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        if (!exceptions.isEmpty()) {
            throw new ListException(exceptions);
        }
    }

    @Override
    public int getChunkSize(String chunkId) {
        try {
            return readChunk(chunkId).length;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Override
    public List<String> getChecksum(String chunkId) throws IOException {

        RandomAccessFile chunk = new RandomAccessFile(CHECKSUM_PATH + File.separator + chunkId, "r");
        List<String> res = new ArrayList<>();
        int size = chunk.readInt();
        byte[] output;
        while (size != -1) {
            output = new byte[size];
            chunk.read(output);
            res.add(new String(output));
            size = chunk.readInt();
        }
        chunk.close();
        return res;
    }

    @Override
    public String[] getFileNames() {
        File[] files = getFiles();
        String[] names = new String[files.length];
        for (int i = 0; i < files.length; i++) {
            names[i] = files[0].getName();
        }
        return names;
    }

    @Override
    public File[] getFiles() {
        try {
            readLock.lock();
            File dir = new File(STORAGE_PATH);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            if (!dir.isDirectory()) {
                return new File[0];
            }
            File[] files = dir.listFiles();
            return files;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getFullCapacity() {
        File dir;
        try {
            dir = createDir(ROOT_PATH);
            return dir.getTotalSpace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public long getUsedCapacity() {
        File dir;
        try {
            dir = createDir(ROOT_PATH);
            return dir.getTotalSpace() - dir.getFreeSpace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

}
