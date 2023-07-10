package org.comp7705.client.utils;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class ProtobufUtil {
    public static List<String> byteStrList2StrList(List<ByteString> byteStrList) {
        ArrayList<String> strList = new ArrayList<>();
        for (ByteString byteStr: byteStrList) {
            strList.add(byteStr.toStringUtf8());
        }
        return strList;
    }
}
