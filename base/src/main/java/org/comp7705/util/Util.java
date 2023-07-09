package org.comp7705.util;

import org.comp7705.protocol.definition.PieceOfChunk;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
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

    public static InetAddress getLocalHostExactAddress() {
        try {
            InetAddress candidateAddress = null;

            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface iface = networkInterfaces.nextElement();
                // 该网卡接口下的ip会有多个，也需要一个个的遍历，找到自己所需要的
                for (Enumeration<InetAddress> inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements(); ) {
                    InetAddress inetAddr = inetAddrs.nextElement();
                    // 排除loopback回环类型地址（不管是IPv4还是IPv6 只要是回环地址都会返回true）
                    if (!inetAddr.isLoopbackAddress()) {
                        if (inetAddr.isSiteLocalAddress()) {
                            // 如果是site-local地址，就是它了 就是我们要找的
                            // ~~~~~~~~~~~~~绝大部分情况下都会在此处返回你的ip地址值~~~~~~~~~~~~~
                            return inetAddr;
                        }

                        // 若不是site-local地址 那就记录下该地址当作候选
                        if (candidateAddress == null) {
                            candidateAddress = inetAddr;
                        }

                    }
                }
            }

            // 如果出去loopback回环地之外无其它地址了，那就回退到原始方案吧
            return candidateAddress == null ? InetAddress.getLocalHost() : candidateAddress;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
