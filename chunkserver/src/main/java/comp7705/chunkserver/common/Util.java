package comp7705.chunkserver.common;

import org.comp7705.protocol.definition.PieceOfChunk;

import java.util.zip.CRC32;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public class Util {

    public static boolean checksum(byte[] value, String checksum) {

        CRC32 crc32 = new CRC32();
        crc32.reset();
        crc32.update(value);
        return String.valueOf(crc32.getValue()).equals(checksum);
    }

    public static boolean checksum(PieceOfChunk pieceOfChunk, String checksum) {
        return checksum(pieceOfChunk.getPiece().toByteArray(), checksum);
    }

    public static boolean checksum(char[] value, char[] checksum) {

        return checksum(String.valueOf(value).getBytes(), String.valueOf(checksum));
    }

    public static String getChecksum(String string) {
        return getChecksum(string.getBytes());
    }

    public static String getChecksum(byte[] value) {
        CRC32 crc32 = new CRC32();
        crc32.reset();
        crc32.update(value);
        return String.valueOf(crc32.getValue());
    }

}
