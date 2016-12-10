package ir.ac.sbu.graph.utils;

/**
 *
 */
public class VIntUtils {

    public static byte[] newByteArray(int size) {
        return new byte[4 + size];
    }

    public static void write(byte[] buf, int i) {
        int pos = (buf[0] << 24) + (buf[1] << 16) + (buf[2] << 8) + (buf[3] << 0);
        if(i >= -112L && i <= 127L) {
            buf[pos ++] = (byte) i;
        } else {
            int len = -112;
            if(i < 0L) {
                i = ~i;
                len = -120;
            }

            for(long tmp = i; tmp != 0L; --len) {
                tmp >>= 8;
            }

            buf[pos ++] = (byte) len;
            len = len < -120?-(len + 120):-(len + 112);

            for(int idx = len; idx != 0; --idx) {
                int shiftbits = (idx - 1) * 8;
                long mask = 255L << shiftbits;
                buf[pos ++] = (byte) ((i & mask) >> shiftbits);
            }
        }
        buf[0] = (byte) ((pos >>> 24) & 0xFF);
        buf[1] = (byte) ((pos >>> 16) & 0xFF);
        buf[2] = (byte) ((pos >>> 8) & 0xFF);
        buf[3] = (byte) ((pos >>> 0) & 0xFF);
    }

    public static int read(byte[] buf, int pos) {
        byte firstByte = buf[pos ++];
        int len = decodeVIntSize(firstByte);
        if(len == 1) {
            return firstByte;
        } else {
            int i = 0;

            for(int idx = 0; idx < len - 1; ++idx) {
                byte b = buf[pos ++];
                i <<= 8;
                i |= (b & 255);
            }

            return isNegativeVInt(firstByte)?~i:i;
        }
    }

    public static boolean isNegativeVInt(byte value) {
        return value < -120 || value >= -112 && value < 0;
    }

    public static int decodeVIntSize(byte value) {
        return value >= -112?1:(value < -120?-119 - value:-111 - value);
    }
}
