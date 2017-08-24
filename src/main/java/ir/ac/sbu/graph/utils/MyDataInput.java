package ir.ac.sbu.graph.utils;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyDataInput implements DataInput {

    List<byte[]> bytesList = new ArrayList<>(3);
    private int length;

    public void add(byte[] bytes) {
        bytesList.add(bytes);
    }

    int bi;
    int li;
    int c;

    public int getLength() {
        return length;
    }

    public void reset(List<byte[]> bytesList, int length) {
        this.bytesList = bytesList;
        this.length = length;
        bi = 0;
        li = 0;
        c = 0;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    public boolean isFull() {
        return c >= length;
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        byte[] bytes = new byte[len];
        System.arraycopy(b, off, bytes, 0, len);
        bytesList.add(bytes);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int count = 0;
        while (true) {
            if (li == bytesList.size())
                break;
            byte[] bytes = bytesList.get(li);
            while (bi < bytes.length && count < n) {
                bi++;
                count++;
            }

            if (count == n)
                break;

            li++;
        }

        return count;
    }

    @Override
    public boolean readBoolean() throws IOException {
        if (readByte() == 0)
            return false;
        return true;
    }

    @Override
    public byte readByte() throws IOException {
        if (li >= bytesList.size())
            throw new ArrayIndexOutOfBoundsException("li = " + li + " bytesList.size = " + bytesList.size());
        byte[] bytes = bytesList.get(li);
        if (bi >= bytes.length) {
            if (li == (bytesList.size() - 1))
                throw new ArrayIndexOutOfBoundsException("bi = " + bi + " bytes.length = " + bi +
                    "li = " + li + " bytesList.size = " + bytesList.size());
            else {
                bytes = bytesList.get(++li);
                bi = 0;
            }
        }

        c ++;
        return bytes[bi++];
    }

    @Override
    public int readUnsignedByte() throws IOException {
        int ch = readByte();
        if (ch < 0)
            throw new EOFException();
        return ch;
    }

    @Override
    public short readShort() throws IOException {
        int ch1 = readByte();
        int ch2 = readByte();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short)((ch1 << 8) + (ch2 << 0));
    }

    @Override
    public int readUnsignedShort() throws IOException {
        int ch1 = readByte();
        int ch2 = readByte();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return ((ch1 << 8) + (ch2 << 0));
    }

    @Override
    public char readChar() throws IOException {
        int ch1 = readByte();
        int ch2 = readByte();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (char)((ch1 << 8) + (ch2 << 0));
    }

    @Override
    public int readInt() throws IOException {
        int ch1 = readByte();
        int ch2 = readByte();
        int ch3 = readByte();
        int ch4 = readByte();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    private byte readBuffer[] = new byte[8];

    @Override
    public long readLong() throws IOException {
        readFully(readBuffer, 0, 8);
        return (((long)readBuffer[0] << 56) +
            ((long)(readBuffer[1] & 255) << 48) +
            ((long)(readBuffer[2] & 255) << 40) +
            ((long)(readBuffer[3] & 255) << 32) +
            ((long)(readBuffer[4] & 255) << 24) +
            ((readBuffer[5] & 255) << 16) +
            ((readBuffer[6] & 255) <<  8) +
            ((readBuffer[7] & 255) <<  0));
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public String readLine() throws IOException {
        throw new UnsupportedOperationException("Unsupported reading string");
    }

    @Override
    public String readUTF() throws IOException {
        throw new UnsupportedOperationException("Unsupported reading string");
    }
}
