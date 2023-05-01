package comp7705.chunkserver.exception;

/**
 * @author Reuze
 * @Date 01/05/2023
 */
public class ChecksumException extends Exception {

    public ChecksumException() {
        super("Checksum could not match chunk content");
    }
}
