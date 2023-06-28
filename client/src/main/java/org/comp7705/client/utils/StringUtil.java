package org.comp7705.client.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public class StringUtil {
    private static StringUtil instance = null;
    private final CRC32 crc32;
    private StringUtil(){
        crc32 = new CRC32();
    }

    public static StringUtil getInstance() {
        if (instance == null) {
            instance = new StringUtil();
        }
        return instance;
    }

    public List<String> getCheckSums(byte[] bytes, int size) {
        ArrayList<String> result = new ArrayList<>();
        for (int off = 0; off < bytes.length; off += size) {
            crc32.reset();
            if (off + size > bytes.length) {
                crc32.update(bytes, off, bytes.length - off);
            } else {
                crc32.update(bytes, off, size);
            }
            result.add(String.valueOf(crc32.getValue()));
        }
        return result;
    }
}
