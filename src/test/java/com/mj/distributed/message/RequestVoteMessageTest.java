package com.mj.distributed.message;

import com.mj.distributed.model.LogEntryWithIndex;
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
                33,3
        );

        ByteBuffer b = requestVoteMessage.serialize();

        RequestVoteMessage readMessage = RequestVoteMessage.deserialize(b) ;

        assertTrue(3 == readMessage.getTerm());
        assertTrue(33 == readMessage.getLastLogIndex());

    }
}