package com.mj.distributed.message;

import com.mj.distributed.model.LogEntry;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class RequestVoteMessageTest {

    @Test
    public void serialize() throws Exception {

        RequestVoteMessage requestVoteMessage = new RequestVoteMessage(
                3,
                "192.168.5.21",
                5050,
                new LogEntry(33, 74)
        );

        ByteBuffer b = requestVoteMessage.serialize();

        RequestVoteMessage readMessage = RequestVoteMessage.deserialize(b) ;

        assertTrue(3 == readMessage.getTerm());
        assertTrue(33 == readMessage.getCommittedLastLogEntry().getIndex());
        assertTrue(74 == ByteBuffer.wrap(readMessage.getCommittedLastLogEntry().getEntry()).getInt());

    }
}