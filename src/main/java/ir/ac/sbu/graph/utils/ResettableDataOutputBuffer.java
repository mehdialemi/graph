package ir.ac.sbu.graph.utils;

import org.apache.hadoop.io.DataOutputBuffer;

import java.io.*;

/**
 * Copied from {@link DataOutputBuffer} but a constructor is added to receive a byte[];
 */
public class ResettableDataOutputBuffer extends DataOutputStream {

    private static class Buffer extends ByteArrayOutputStream {
        public byte[] getData() { return buf; }
        public int getLength() { return count; }


        public Buffer() {
            super();
        }

        public Buffer(int size) {
            super(size);
        }

        public void reset(byte[] buf, int count) {
            this.buf = buf;
            this.count = count;
        }

        public void write(DataInput in, int len) throws IOException {
            int newcount = count + len;
            if (newcount > buf.length) {
                byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
                System.arraycopy(buf, 0, newbuf, 0, count);
                buf = newbuf;
            }
            in.readFully(buf, count, len);
            count = newcount;
            buf[0] = (byte) ((count >>> 24) & 0xFF);
            buf[1] = (byte) ((count >>> 16) & 0xFF);
            buf[2] = (byte) ((count >>> 8) & 0xFF);
            buf[3] = (byte) ((count >>> 0) & 0xFF);
        }
    }

    private Buffer buffer;

    /** Constructs a new empty buffer. */
    public ResettableDataOutputBuffer() {
        this(new Buffer());
    }

    public ResettableDataOutputBuffer(int size) {
        this(new Buffer(size));
    }

    private ResettableDataOutputBuffer(Buffer buffer) {
        super(buffer);
        this.buffer = buffer;

    }

    /** Returns the current contents of the buffer.
     *  Data is only valid to {@link #getLength()}.
     */
    public byte[] getData() { return buffer.getData(); }

    /** Returns the length of the valid data currently in the buffer. */
    public int getLength() { return buffer.getLength(); }

    /** Resets the buffer to empty. */
    public ResettableDataOutputBuffer reset() {
        this.written = 0;
        buffer.reset();
        return this;
    }

    public void reset(byte[] buf) {
        buffer.reset(buf, 4 + ((buf[0] << 24) + (buf[1] << 16) + (buf[2] << 8) + (buf[3] << 0)));
        this.written = buffer.getLength();
    }

    /** Writes bytes from a DataInput directly into the buffer. */
    public void write(DataInput in, int length) throws IOException {
        buffer.write(in, length);
    }

    /** Write to a file stream */
    public void writeTo(OutputStream out) throws IOException {
        buffer.writeTo(out);
    }
}
