package com.mj.distributed.message;

import com.mj.distributed.model.LogEntryWithIndex;
import org.junit.jupiter.api.Test;


import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AppendEntriesMessageTest {

    @Test
    public void serialize() throws Exception {

        AppendEntriesMessage msg = new AppendEntriesMessage(4,"localhost:5001",1,
                14,3, 6);
        LogEntryWithIndex e = new LogEntryWithIndex(3, 0,4);

        msg.addLogEntry(e);

        ByteBuffer b = msg.serialize() ;

        AppendEntriesMessage t = AppendEntriesMessage.deserialize(b) ;

        assertEquals(4, t.getTerm());
        assertEquals(6, t.getLeaderCommitIndex());

    }

}