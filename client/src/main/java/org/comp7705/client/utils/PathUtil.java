package org.comp7705.client.utils;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class PathUtil {
    public static List<String> splitPath(String path) {
        String sepExp = "\\[/\\\\]";
        return Arrays.asList(path.split(sepExp));
    }

    public static String getParentPath(String path) {
        List<String> pathFrac = splitPath(path);
        pathFrac.remove(pathFrac.size() - 1);
        return String.join(File.separator, pathFrac);
    }

    public static String getCurrentName(String path) {
        List<String> pathFrac = splitPath(path);
        return pathFrac.get(pathFrac.size() - 1);
    }
}
