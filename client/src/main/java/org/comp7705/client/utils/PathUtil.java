package org.comp7705.client.utils;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class PathUtil {

    public static String getParentPath(String path) {
        int lastSep = path.lastIndexOf(File.separator);
        return path.substring(0, lastSep) + File.separator;
    }

    public static String getCurrentName(String path) {
        int lastSep = path.lastIndexOf(File.separator);
        return path.substring(lastSep + 1);
    }
}
