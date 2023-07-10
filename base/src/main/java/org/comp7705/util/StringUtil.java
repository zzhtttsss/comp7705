package org.comp7705.util;

public class StringUtil {

    public static String concatString(CharSequence... strings) {

        StringBuilder sb = new StringBuilder();
        for (CharSequence str : strings) {
            sb.append(str);
        }
        return sb.toString();
    }
}
