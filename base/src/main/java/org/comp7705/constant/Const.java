package org.comp7705.constant;

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
    public final static int PieceSize = PieceMBNum * MB;//PieceMBNum * MB;
    public final static int ChunkSize  = ChunkMBNum * MB;

    public final static int CheckSumSize = 10;

    // zk
    public final static String ZkIp = "127.0.0.1";
    public final static int ZkPort = 2181;
    public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 5000;
    public static final int DEFAULT_SESSION_TIMEOUT_MS = 60000;
    public static final String SESSION_TIMEOUT_KEY = "zk.sessionTimeoutMs";
    public static final int RETRY_TIMES = 3;
    public static final int RETRY_SLEEP_MS = 1000;
    public static final String ROOT_PATH = "/dfs";
}
