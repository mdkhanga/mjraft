package com.mj.distributed.model;


import java.io.*;
import java.nio.ByteBuffer;

public class LogEntry {

    private int index;
    private int term ;
    private byte[] entry;

    public LogEntry(int term, int index, int value) {
        this.term = term;
        this.index = index;
        this.entry = ByteBuffer.allocate(4).putInt(value).array();
    }

    public LogEntry(int term, int index, byte[] val) {
        this.term = term;
        this.index = index;
        this.entry = val;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(out);
        d.writeInt(term);
        d.writeInt(index);
        int size = entry.length;
        d.writeInt(size);
        if (size > 0) {
            d.write(entry);
        }
        byte[] ret = out.toByteArray();
        out.close();
        d.close();
        return ret;
    }

    public static LogEntry fromBytes(byte[] bytes) throws IOException {

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);
        int term = din.readInt();
        int index = din.readInt() ;
        // int v = din.readInt();
        int size = din.readInt();

        byte[] v = new byte[size];

        if (size > 0) {
            din.read(v,0, size);
        }

        return new LogEntry(term, index, v);

    }

    public int getIndex() {
        return index;
    }

    public byte[] getEntry() {
        return entry;
    }
}
