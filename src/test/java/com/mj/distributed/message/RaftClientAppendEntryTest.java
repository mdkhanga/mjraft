package com.mj.distributed.message;



import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class RaftClientAppendEntryTest {

    @Test
    public void serialize() throws Exception {

        RaftClientAppendEntry rcAe = new RaftClientAppendEntry(ByteBuffer.allocate(4).putInt(23).array());
        ByteBuffer b = rcAe.serialize() ;

        RaftClientAppendEntry rcAeRead = RaftClientAppendEntry.deserialize(b) ;

        ByteBuffer bR = ByteBuffer.wrap(rcAeRead.getValue());

        assertTrue(23 == bR.getInt());


    }
}