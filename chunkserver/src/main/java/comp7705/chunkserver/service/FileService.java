package comp7705.chunkserver.service;

import org.comp7705.protocol.definition.PieceOfChunk;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public interface FileService {

    public void storeChunk(PieceOfChunk pieceOfChunk, String chunkId, int chunkSize, String checksum) throws IOException;

    public void finishStoreChunk(String chunkId) throws IOException;

    public void renameChunk(String oldName, String newName) throws IOException;

    public byte[] readChunk(String chunkId) throws Exception;

    public void deleteChunk(String chunkId) throws Exception;

    public void deleteChunk(List<String> chunkIds) throws Exception;

    public int getChunkSize(String chunkId);

    public List<String> getChecksum(String chunkId) throws IOException;

    public String[] getFileNames();

    public File[] getFiles();

    public long getFullCapacity();

    public long getUsedCapacity();

}
