package com.mj.distributed.model;


import java.io.*;
import java.nio.ByteBuffer;

public class LogEntry {


    private int term ;
    private byte[] entry;



    public LogEntry(int term,  byte[] val) {
        this.term = term;
        this.entry = val;
    }



    public int getTerm() {
        return term;
    }

    public byte[] getEntry() {
        return entry;
    }
}
