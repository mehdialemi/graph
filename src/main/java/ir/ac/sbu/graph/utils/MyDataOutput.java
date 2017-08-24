package ir.ac.sbu.graph.utils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MyDataOutput implements DataOutput {

    final List<byte[]> bytesList = new ArrayList<>();
    private int bucketSize;
    int bi = 0;  // byte index
    int length = 0;
    byte[] bytes;

    public MyDataOutput() {
        this(5);
    }

    public MyDataOutput(int bucketSize) {
        if (bucketSize <= 0)
            bucketSize = 1;
        this.bucketSize = bucketSize;
        bytes = new byte[bucketSize];
        bytesList.add(bytes);
    }

    public final List<byte[]> getBytesList() {
        return bytesList;
    }

    public int length() {
        return length;
    }

    public void add(byte b) {
        if (bi == bucketSize) {
            bytes = new byte[bucketSize];
            bytesList.add(bytes);
            bi = 0;
        }
        bytes[bi ++] = b;
        length ++;
    }

    public byte get(int i) {
        if (i >= length)
            throw new ArrayIndexOutOfBoundsException("index: " + i + " length: " + length);
        int li = i / bucketSize;
        return bytesList.get(li) [i % bucketSize];
    }

    @Override
    public void write(int b) throws IOException {
        add((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        for(int i = off ; i < len ; i ++)
            add(b[i]);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        add((byte) (v ? 1 : 0));
    }

    @Override
    public void writeByte(int v) throws IOException {
        add((byte) v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        add((byte) ((v >>> 8) & 0xFF));
        add((byte) ((v >>> 0) & 0xFF));
    }

    @Override
    public void writeChar(int v) throws IOException {
        add((byte) ((v >>> 8) & 0xFF));
        add((byte) ((v >>> 0) & 0xFF));
    }

    @Override
    public void writeInt(int v) throws IOException {
        add((byte) ((v >>> 24) & 0xFF));
        add((byte) ((v >>> 16) & 0xFF));
        add((byte) ((v >>>  8) & 0xFF));
        add((byte) ((v >>>  0) & 0xFF));
    }

    @Override
    public void writeLong(long v) throws IOException {
        add ((byte)(v >>> 56));
        add ((byte)(v >>> 48));
        add ((byte)(v >>> 40));
        add ((byte)(v >>> 32));
        add ((byte)(v >>> 24));
        add ((byte)(v >>> 16));
        add ((byte)(v >>>  8));
        add ((byte)(v >>>  0));
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        throw new UnsupportedOperationException("Writing string is not supported");
    }

    @Override
    public void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0 ; i < len ; i++) {
            int v = s.charAt(i);
            add ((byte) ((v >>> 8) & 0xFF));
            add ((byte) ((v >>> 0) & 0xFF));
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        throw new UnsupportedOperationException("Writing string is not supported");
    }
}
