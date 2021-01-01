package com.mj.distributed.model;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class LogEntryTest {

    @Test
    void toBytesInt() throws Exception {

        LogEntry e1 = new LogEntry(3, 47);

        byte[] e1bytes = e1.toBytes() ;

        LogEntry e1copy = LogEntry.fromBytes(e1bytes);

        assertEquals(3, e1copy.getIndex());
        int ret = ByteBuffer.wrap(e1copy.getEntry()).getInt();
        assertEquals(47,ret);

    }

    @Test
    void toBytesString() throws Exception {

        LogEntry e1 = new LogEntry(7, "Hello".getBytes());

        byte[] e1bytes = e1.toBytes() ;

        LogEntry e1copy = LogEntry.fromBytes(e1bytes);

        assertEquals(7, e1copy.getIndex());
        assertEquals("Hello",new String(e1copy.getEntry()));

    }
}