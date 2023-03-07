package com.mj.distributed.model;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class LogEntryTest {

    @Test
    void toBytesInt() throws Exception {

        LogEntryWithIndex e1 = new LogEntryWithIndex(2, 3, 47);

        byte[] e1bytes = e1.toBytes() ;

        LogEntryWithIndex e1copy = LogEntryWithIndex.fromBytes(e1bytes);

        assertEquals(3, e1copy.getIndex());
        int ret = ByteBuffer.wrap(e1copy.getEntry()).getInt();
        assertEquals(47,ret);
        assertEquals(2, e1copy.getTerm());

    }

    @Test
    void toBytesString() throws Exception {

        LogEntryWithIndex e1 = new LogEntryWithIndex(4, 7, "Hello".getBytes());

        byte[] e1bytes = e1.toBytes() ;

        LogEntryWithIndex e1copy = LogEntryWithIndex.fromBytes(e1bytes);

        assertEquals(7, e1copy.getIndex());
        assertEquals("Hello",new String(e1copy.getEntry()));
        assertEquals(4, e1copy.getTerm());

    }
}