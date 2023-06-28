package org.comp7705.client.utils;

import java.io.File;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public class Const {

    public final static String ChunkIdString   = "chunkId";
    public final static String AddressString   = "addresses";
    public final static String ChunkSizeString = "chunkSize";
    public final static String CheckSumString  = "checkSum";

    public final static String PATH = System.getProperty("user.dir") + File.separator + System.getProperty("pathPrefix");

    public final static int CopySendType   = 0;
    public final static int MoveSendType   = 1;
    public final static int AddSendType    = 2;
    public final static int DeleteSendType = 3;

    public final static int KB         = 1024;
    public final static int MB         = 1024 * KB;
    public final static int ChunkMBNum = 64;
    public final static int PieceMBNum = 1;
    public final static int PieceSize = 5;//PieceMBNum * MB;
    public final static int ChunkSize  = ChunkMBNum * MB;

    public final static int CheckSumSize = 10;

}
