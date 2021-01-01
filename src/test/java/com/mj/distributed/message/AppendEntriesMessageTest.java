package com.mj.distributed.message;

import com.mj.distributed.model.LogEntry;
import org.junit.jupiter.api.Test;


import java.nio.ByteBuffer;

public class AppendEntriesMessageTest {

    @Test
    public void serialize() throws Exception {

        AppendEntriesMessage msg = new AppendEntriesMessage(1,1);
        LogEntry e = new LogEntry(0,4);

        msg.addLogEntry(e);

        ByteBuffer b = msg.serialize() ;

        AppendEntriesMessage t = AppendEntriesMessage.deserialize(b) ;

    }

}